package hu.oe.bakonyi.bkk

import hu.oe.bakonyi.bkk.BkkDataProcesser.ssc
import hu.oe.bakonyi.bkk.BkkDataProcesserV2.longTermModel
import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV4, MLReadyBkkModel}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesserV2 {
  System.setProperty("spark.driver.allowMultipleContexts", "true")
  val log : Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-learning")
  val ssc2 = new StreamingContext(SparkContext.getOrCreate(),Seconds(1))
  ssc2.sparkContext.setLogLevel("ERROR")
  ssc2.checkpoint("D:\\Spark\\ml")

  val pmmlPath = "D:\\DEV\\pmml\\streamingModel.pmml"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[MlReadyBkkModelDeserializator],
    "group.id" -> "BKKLearningTopic",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("BKKLearningTopic")

  var longTermModel = new StreamingLinearRegressionWithSGD()
  longTermModel.setInitialWeights(Vectors.zeros(9))

  /*
  learningModelStream.foreachRDD(
    rdd =>{
      rdd.foreach(x=>println(s"Feature: ${x.features} @ ${x.label}"))
    }
  )
   */

  def main(args: Array[String]): Unit = {
    println("Continuous learning has started")
    val learningModelStream: DStream[LabeledPoint] = KafkaUtils.createDirectStream(
      ssc2,
      PreferConsistent,
      Subscribe[String,LabeledPoint](topics, kafkaParams)
    ).map(_.value)

    longTermModel.trainOn(learningModelStream)
    println(s"Learning has been completed")
    println(s"Saving the model")
    longTermModel.latestModel().toPMML(pmmlPath)
    println(s"Model was saved to ${pmmlPath}")
    ssc2.start()
    ssc2.awaitTermination()
  }

  /*
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

  def bkkv4DStreamMapper(v2 : BkkBusinessDataV2) : BkkBusinessDataV4 ={
    val arrivalDiff = v2.arrivalDiff
    val departureDiff = v2.departureDiff
    val value: Double = ((arrivalDiff + departureDiff)) / 2
    var stopId = v2.stopId.split("_")(1)
    stopId = stopId.substring(1, stopId.length)
    BkkBusinessDataV4(
      v2.month,
      v2.dayOfWeek,
      v2.hour,
      v2.routeId.split("_")(1).toInt,
      stopId.toInt,
      v2.temperature,
      v2.humidity,
      v2.pressure,
      v2.snow,
      v2.rain,
      v2.visibility,
      v2.alert,
      value
    )
  }
 */

}