package hu.oe.bakonyi.bkk

import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV3}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{LabeledPoint, VectorIndexer}
import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesser {
  val log : Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val conf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.sparkContext.setLogLevel("ERROR")

  val modelDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/model"

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

  val stream = KafkaUtils.createDirectStream(
    ssc,
    PreferConsistent,
    Subscribe[String,BkkBusinessDataV2](topics, kafkaParams)
  ).map(_.value)

  val sparkSession =  SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  stream.foreachRDD(
    foreachFunc = rdd => {
      if (!rdd.isEmpty()) {

        val bkkData: Dataset[BkkBusinessDataV2] = sparkSession.createDataset(rdd)
        printf(s"Egy RDD groupby után: ${System.lineSeparator()}")

        val aggregatedAvgData = bkkData.groupBy($"month", $"dayOfWeek", $"lastUpdateTime", $"routeId", $"tripId", $"stopId").avg()
        aggregatedAvgData.show(10, false)
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
        bkkv3Data.show(10, false)

        /*
        try {
          val smodel = org.apache.spark.ml.PipelineModel.load(modelDirectory)
          smodel.transform(bkkv3Data).write.save(modelDirectory)
        }catch {
          case ex : UnsupportedOperationException =>{
            val model = lr.fit(bkkv3Data)
            model.write.overwrite().save(modelDirectory)
          }
        }
        */

        val featureVector: Dataset[LabeledPoint] = bkkv3Data.map(x => LabeledPoint(x.value, transformVector(x)))

        val featureIndexer = new VectorIndexer()
          .setInputCol("features")
          .setOutputCol("indexedFeatures")
          .setMaxCategories(100)
          .fit(featureVector)

        val dt = new DecisionTreeRegressor()
          .setLabelCol("label")
          .setFeaturesCol("indexedFeatures")

        val pipeline = new Pipeline()
          .setStages(Array(featureIndexer, dt))

        val model = pipeline.fit(featureVector)
        pipeline.save(modelDirectory)

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