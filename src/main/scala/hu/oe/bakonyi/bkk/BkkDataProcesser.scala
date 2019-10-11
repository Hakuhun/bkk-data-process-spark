package hu.oe.bakonyi.bkk

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

//https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
//https://spark.apache.org/docs/latest/streaming-programming-guide.html
//https://stackoverflow.com/questions/36421619/whats-the-meaning-of-dstream-foreachrdd-function
object BkkDataProcesser {

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-process")
  val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bkk-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics: Array[String] = Array("bkk")

  val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    PreferConsistent,
    Subscribe[String,String](topics, kafkaParams)
  )

  val records: DStream[(String, String)] = stream.map(record => (record.key, record.value))

  stream.foreachRDD{rdd=>
    rdd.first().value()
  }

  stream.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // some time later, after outputs have completed
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }

  def main(args: Array[String]): Unit = {
    print("ASD"+System.lineSeparator())
    ssc.start()
    ssc.awaitTermination()
  }


}
