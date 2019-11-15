package hu.oe.bakonyi.bkk

import java.io.File

import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV4}
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.spark.SparkSupport._
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import resource._

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesserV2 {
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

/*
  val asd: DStream[BkkBusinessDataV4] = stream.map(data => bkkv4DStreamMapper(data))
  val asdasd: DStream[Iterable[BkkBusinessDataV2]] = stream.transform((rdd) =>{
      val grouped = rdd.groupBy(data => Array(data.month, data.dayOfWeek, data.hour, data.lastUpdateTime, data.routeId, data.tripId, data.stopId))

  })*/

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
          }.filter(x=>x.label > 0 && x.label < 1500000)

        printf(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")
        log.info(s"RDD data after aggregating and grouping: ${System.lineSeparator()}")

        bkkv3Data.show(10, false)

        if(bkkv3Data.count() > 0) {


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

}