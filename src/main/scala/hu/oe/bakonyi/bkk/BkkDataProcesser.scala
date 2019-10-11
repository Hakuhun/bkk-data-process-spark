package hu.oe.bakonyi.bkk

import hu.oe.bakonyi.bkk.model.BkkBusinessData
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
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
  //ssc.sparkContext.setLogLevel("ERROR")

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
    Subscribe[String,BkkBusinessData](topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value))

  stream.foreachRDD { rdd =>
    //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // some time later, after outputs have completed
    //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    rdd.foreach { data =>
      log.info(s"Adat: ${data.topic()} @ ${data.value()} ${System.lineSeparator()}")
    }
  }


  def main(args: Array[String]): Unit = {
    log.info("Spark JOB for BKK "+System.lineSeparator())
    ssc.start()
    ssc.awaitTermination()
  }

}