package hu.oe.bakonyi.bkk

import com.google.gson.Gson
import hu.oe.bakonyi.bkk.model._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, mllib}


//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesser {

  //System.setProperty("hadoop.home.dir", "C:\\Spark")
  val log: Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc = new StreamingContext(conf, Seconds(1))

  ssc.sparkContext.setLogLevel("ERROR")
  ssc.checkpoint("D:\\Spark\\processer")

  //val pipelineDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/pipeline"
  val pipelineDirectory = "D:\\pipeline"
  val modelDirectory = "D:\\DEV\\nodel"
  val pmmlPath = "D:\\DEV\\pmml\\basicmodell.pmml"
  val csvDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/bkk.csv"

  var model: PipelineModel = _
  var newModel: PipelineModel = _
  var pipeline: Pipeline = _

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[BkkDataDeserializer],
    "group.id" -> "bkk",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val inputKafkaTopics = Array("bkk")

  val stream: DStream[BkkBusinessDataV2] = KafkaUtils.createDirectStream(
    ssc,
    PreferConsistent,
    Subscribe[String, BkkBusinessDataV2](inputKafkaTopics, kafkaParams)
  ).map(_.value)

  val sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  stream.foreachRDD(
    foreachFunc = rdd => {
      if (!rdd.isEmpty()) {

        printf("Streaming pipeline has started.")

        val bkkData: Dataset[BkkBusinessDataV2] = sparkSession.createDataset(rdd)

        val aggregatedAvgData = bkkData.groupBy($"month", $"dayOfWeek", $"lastUpdateTime", $"routeId", $"tripId", $"stopId", $"hour")
          .agg(
            Map(
              "temperature" -> "avg",
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

        val bkkv4Data: Dataset[BkkBusinessDataV4] = aggregatedAvgData
          .map {
            case row: GenericRow => bkkv4Mapper(row)
          }.filter((x) => (x.label > 0 && x.label < 1500000))

        val backup: Dataset[BkkBusinessDataV4] = aggregatedAvgData
          .map {
            case row: GenericRow => bkkv4Mapper(row)
          }

        printf(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")
        log.info(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")

        bkkv4Data.show(10, false)

        if (bkkv4Data.count() > 0) {
          val backupKafkaDf = backup.map(x => new Gson().toJson(x)).alias("value")

          try {
            backupKafkaDf.write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "refinedBKK").save()
          } catch {
            case unknownError: UnknownError => {
              printf(unknownError.getMessage)
              unknownError.printStackTrace()
            }
          }

          val learningKafkaDf = bkkv4Data.map(x => LabeledPoint(x.label, transformVector(x))).map(x => new Gson().toJson(x)).alias("value")

          try {
            learningKafkaDf.write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "BKKLearningTopic").save()
          } catch {
            case unknownError: UnknownError => {
              printf(unknownError.getMessage)
              unknownError.printStackTrace()
            }
          }
          /*
                    val splits = bkkv4Data.randomSplit(Array(0.7, 0.3))

                    val trainingData = splits(0)
                    val testData = splits(1)

                        val assembler = new VectorAssembler()
                         .setInputCols(Array("routeId", "stopId", "month","dayOfWeek","hour","temperature","humidity","pressure","rain","snow","visibility"))
                         .setOutputCol("features")


                        val dt = new DecisionTreeRegressor()
                          .setLabelCol("label")
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
                          case iie: org.apache.hadoop.mapred.InvalidInputException => {
                            pipeline.setStages(Array(assembler,dt))
                            printf(iie.getMessage)
                          }
                          case unknownError: UnknownError => {
                            printf(unknownError.getMessage)
                          }
                        }

                        newModel = pipeline.fit(trainingData)

                        // Make predictions.
                        val predictions: DataFrame = newModel.transform(testData)

                        // Select example rows to display.
                        println(s"Predictions based on ${System.currentTimeMillis()} time train: ")
                        predictions.show(10, false)

                        // Select (prediction, true label) and compute test error
                        val evaluator = new MulticlassClassificationEvaluator()
                          .setLabelCol("label")
                          .setPredictionCol("prediction")
                          .setMetricName("accuracy")
                        val accuracy = evaluator.evaluate(predictions)

                        println("Test Error = " + (1.0 - accuracy))

                        pipeline.write.overwrite().save(pipelineDirectory)
                        newModel.write.overwrite().save(modelDirectory)

                        var schema: StructType = trainingData.schema

                        var pmml = new PMMLBuilder(schema, newModel)
                        JAXBUtil.marshalPMML(pmml.build(), new StreamResult(new FileOutputStream(pmmlPath)))
          */
        } else {
          println("No fitable values left after aggregating and preprocessing.")
        }
      }
    }
  )

  def main(args: Array[String]): Unit = {
    log.info("Spark JOB for BKK " + System.lineSeparator())
    ssc.start()
    ssc.awaitTermination()
  }


  //Departure nézéser labelként
  def bkkv4Mapper(row: GenericRow): BkkBusinessDataV4 = {
    //val arrivalDiff = row.getAs[Double]("sum(arrivalDiff)")
    val departureDiff = row.getAs[Double]("sum(departureDiff)")
    val value: Double = departureDiff
    //val value: Double = departureDiff
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

  def transformVector(x: BkkBusinessDataV4): mllib.linalg.Vector = {
    Vectors.dense(
      x.routeId,
      x.stopId,
      x.month,
      x.dayOfWeek,
      x.hour,
      x.alert,
      x.temperature,
      x.rain + x.snow,
      x.visibility
    )
  }
}

