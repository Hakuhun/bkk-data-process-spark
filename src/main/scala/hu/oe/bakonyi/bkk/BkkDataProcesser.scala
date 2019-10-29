package hu.oe.bakonyi.bkk

import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV3}
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesser {
  val log : Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val pipelineDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/pipeline"
  val modelDirectory =  "/mnt/D834B3AF34B38ECE/DEV/hadoop/model"

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

        val bkkData: Dataset[BkkBusinessDataV2] = sparkSession.createDataset(rdd)

        val aggregatedAvgData = bkkData.groupBy($"month", $"dayOfWeek", $"lastUpdateTime", $"routeId", $"tripId", $"stopId").avg()
        val bkkv3Data: Dataset[BkkBusinessDataV3] = aggregatedAvgData
          .map {
            case row: GenericRow => BkkBusinessDataV3(
              row.getAs[Int]("month"),
              row.getAs[Int]("dayOfWeek"),
              row.getAs[String]("routeId").split("_")(1).toInt,
              row.getAs[Double]("avg(temperature)"),
              row.getAs[Double]("avg(humidity)"),
              row.getAs[Double]("avg(pressure)"),
              row.getAs[Double]("avg(snow)"),
              row.getAs[Double]("avg(rain)"),
              row.getAs[Double]("avg(visibility)"),
              ((row.getAs[Double]("avg(departureDiff)") + row.getAs[Double]("avg(arrivalDiff)")) / 2)
            )
          }

        printf(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")
        log.info(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")

        bkkv3Data.show(10, false)
        //log.info(bkkv3Data.show(10, false).toString)

        val featureVector: Dataset[LabeledPoint] = bkkv3Data.map(x => LabeledPoint(x.value, transformVector(x)))

        printf(s"FeatureVector definition: ${System.lineSeparator()}")
        featureVector.show(2,false)

        val splits: Array[Dataset[LabeledPoint]] = featureVector.randomSplit(Array(0.7, 0.3))
        val trainingData: Dataset[LabeledPoint] = splits(0)
        val testData: Dataset[LabeledPoint] = splits(1)

        val dt = new DecisionTreeRegressor()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setImpurity("variance")
          .setMaxDepth(20)
          .setMaxBins(32)
          .setMinInstancesPerNode(5)

        var pipeline = new Pipeline()

        try{
          pipeline = Pipeline.read.load(pipelineDirectory)
        }catch {
          case iie : InvalidInputException =>{
            pipeline.setStages(Array(dt))
            printf(iie.getMessage)
          }
          case unknownError: UnknownError =>{
            printf(unknownError.getMessage)
          }
        }

        val model = pipeline.fit(trainingData)



        // Make predictions.
        val predictions = model.transform(testData)

        print(s"Predictions based on ${System.currentTimeMillis()} time train: ${System.lineSeparator()}")
        // Select example rows to display.
        predictions.show(5, false)

        // Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        println("Test Error = " + (1.0 - accuracy))

        pipeline.write.overwrite().save(pipelineDirectory)
        model.write.overwrite().save(modelDirectory)
      }
    }
  )

  def main(args: Array[String]): Unit = {
    log.info("Spark JOB for BKK "+System.lineSeparator())
    ssc.start()
    ssc.awaitTermination()
  }

  def transformVector(x:BkkBusinessDataV3) : linalg.Vector  = {
    Vectors.dense(
      x.routeId,
      x.month,
      x.dayOfWeek,
      x.temperature,
      x.humidity,
      x.rain,
      x.pressure,
      x.rain,
      x.snow,
      x.visibility)
  }

}