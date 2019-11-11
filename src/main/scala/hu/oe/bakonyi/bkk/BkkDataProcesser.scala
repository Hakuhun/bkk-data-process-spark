package hu.oe.bakonyi.bkk

import java.io.{File, FileOutputStream}

import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV4}
import javax.xml.transform.stream.StreamResult
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.spark.SparkSupport._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.PMMLBuilder
import resource._

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesser {
  val log : Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val pipelineDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/pipeline"
  val modelDirectory =  "/mnt/D834B3AF34B38ECE/DEV/hadoop/model"
  val csvDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/bkk.csv"
  val zipPrefix = "jar:file:"
  val mleapPath = "/mnt/D834B3AF34B38ECE/DEV/bkk-containers/bkk-mleap-api/model/bkk-base-pipeline.zip"
  val pmmlPath = "/mnt/D834B3AF34B38ECE/DEV/hadoop/basicmodel.pmml"

  var model : PipelineModel = _
  var newModel : PipelineModel = _
  var pipeline : Pipeline = _

  val lr = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.02)
    .setElasticNetParam(0.3)


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[BkkDataDeserializer],
    "group.id" -> "bkk",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("bkk")

  val stream: DStream[BkkBusinessDataV2] = KafkaUtils.createDirectStream(
    ssc,
    PreferConsistent,
    Subscribe[String,BkkBusinessDataV2](topics, kafkaParams)
  ).map(_.value)

  val sparkSession: SparkSession =  SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  stream.foreachRDD(
    foreachFunc = rdd => {
      if (!rdd.isEmpty()) {

        printf("Streaming pipeline has started.")

        val bkkData: Dataset[BkkBusinessDataV2] = sparkSession.createDataset(rdd)

        val aggregatedAvgData = bkkData.groupBy($"month", $"dayOfWeek", $"lastUpdateTime", $"routeId", $"tripId", $"stopId", $"hour")
          .agg(
            Map(
              "temperature" ->"avg",
              "humidity" -> "avg",
              "pressure" -> "avg",
              "snow" -> "avg",
              "rain" -> "avg",
              "visibility" -> "avg",
              "arrivalDiff" -> "sum",
              "departureDiff" -> "sum",
              "alert" -> "max",
              "value" -> "avg"
            )
          )

        val bkkv3Data: Dataset[BkkBusinessDataV4] = aggregatedAvgData
          .map {
            case row: GenericRow => bkkv4Mapper(row)
          }.filter(x=>x.value > 0 && x.value < 1500000)

        printf(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")
        log.info(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")

        bkkv3Data.show(10, false)

        if(bkkv3Data.count() > 0) {

          //val splits: Array[Dataset[LabeledPoint]] = featureVector.randomSplit(Array(0.7, 0.3))
          val splits = bkkv3Data.randomSplit(Array(0.7, 0.3))

          val trainingData = splits(0)
          val testData = splits(1)

          val assembler = new VectorAssembler()
            .setInputCols(Array("routeId", "stopId", "month","dayOfWeek","hour","temperature","humidity","pressure","rain","snow","visibility"))
            .setOutputCol("features")

          val dt = new DecisionTreeRegressor()
            .setLabelCol("value")
            .setFeaturesCol("features")
            .setImpurity("variance")
            .setMaxDepth(30)
            .setMaxBins(32)
            .setMinInstancesPerNode(5)

          pipeline = new Pipeline()

          try {
            model = PipelineModel.load(modelDirectory)
            pipeline.setStages(model.stages)
          } catch {
            case iie: InvalidInputException => {
              pipeline.setStages(Array(assembler,dt))
              printf(iie.getMessage)
            }
            case unknownError: UnknownError => {
              printf(unknownError.getMessage)
            }
          }

          newModel = pipeline.fit(trainingData)

          // Make predictions.
          val predictions: DataFrame = model.transform(testData)

          // Select example rows to display.
          println(s"Predictions based on ${System.currentTimeMillis()} time train: ")
          predictions.show(10, false)

          // Select (prediction, true label) and compute test error
          val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("value")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
          val accuracy = evaluator.evaluate(predictions)

          println("Test Error = " + (1.0 - accuracy))

          //pipeline.write.overwrite().save(pipelineDirectory)
          newModel.write.overwrite().save(modelDirectory)

          var schema: StructType = trainingData.schema

          var pmml = new PMMLBuilder(schema, newModel)
          JAXBUtil.marshalPMML(pmml.build(), new StreamResult(new FileOutputStream(pmmlPath)))

        }else{
          println("No fitable values left after aggregating and preprocessing.")
        }
      }
    }
  )

  def exportToMlLean(model: PipelineModel, predictions : DataFrame,  path : String) : Unit ={
    var fileToSave : File = new File(path)

    if(fileToSave.exists()){
      FileUtils.forceDelete(new File(path))
    }

    for(bf <- managed(BundleFile(zipPrefix+path))) {
      model.writeBundle.format(SerializationFormat.Json).save(bf)
    }
  }

  def main(args: Array[String]): Unit = {
    log.info("Spark JOB for BKK "+System.lineSeparator())
    ssc.start()
    ssc.awaitTermination()
  }

  def bkkv4Mapper(row:GenericRow) : BkkBusinessDataV4 ={
    val arrivalDiff = row.getAs[Double]("sum(arrivalDiff)")
    val departureDiff =row.getAs[Double]("sum(departureDiff)")
    val value: Double = ((arrivalDiff + departureDiff)) / 2
    var stopId = row.getAs[String]("stopId").split("_")(1)
    stopId = stopId.substring(1, stopId.length)
    BkkBusinessDataV4(
      row.getAs[Int]("month"),
      row.getAs[Int]("dayOfWeek"),
      row.getAs[Int]("hour"),
      row.getAs[String]("routeId").split("_")(1).toInt,
      stopId.toInt,
      row.getAs[Double]("avg(temperature)"),
      row.getAs[Double]("avg(humidity)"),
      row.getAs[Double]("avg(pressure)"),
      row.getAs[Double]("avg(snow)"),
      row.getAs[Double]("avg(rain)"),
      row.getAs[Double]("avg(visibility)"),
      row.getAs[Byte]("max(alert)"),
      value
    )
  }

}