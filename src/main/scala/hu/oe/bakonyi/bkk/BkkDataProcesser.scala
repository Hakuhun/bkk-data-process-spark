package hu.oe.bakonyi.bkk

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.Deserializer

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
object BkkDataProcesser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc = new StreamingContext(conf, Seconds(1))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("bkk")

  val stream = KafkaUtils.createDirectStream(
    ssc,
    PreferConsistent,
    Subscribe(topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value))

  def main(args: Array[String]): Unit = {

  }


}
