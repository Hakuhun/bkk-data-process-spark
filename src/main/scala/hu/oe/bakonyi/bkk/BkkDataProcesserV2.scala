package hu.oe.bakonyi.bkk

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object BkkDataProcesserV2 {
  System.setProperty("spark.driver.allowMultipleContexts", "true")
  val log : Logger = Logger.getLogger(BkkDataProcesser.getClass)

  val pmmlPath = "D:\\DEV\\pmml\\streamingModel.pmml"
  val pmmlDir = "D:\\DEV\\pmml\\"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("bkk-learning")
    val ssc2 = new StreamingContext(SparkContext.getOrCreate(),Seconds(1))
    ssc2.sparkContext.setLogLevel("ERROR")
    ssc2.checkpoint("D:\\Spark\\ml")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[MlReadyBkkModelDeserializator],
      "group.id" -> "BKKLearningTopic",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("BKKLearningTopic")
    val learningModelStream: DStream[LabeledPoint] = KafkaUtils.createDirectStream(
      ssc2,
      PreferConsistent,
      Subscribe[String,LabeledPoint](topics, kafkaParams)
    ).map(_.value).cache()

    val trainingData = learningModelStream.transform(rdd => rdd.randomSplit(Array(0.7, 0.3))(0))
    val testingData = learningModelStream.transform(rdd => rdd.randomSplit(Array(0.7, 0.3))(1))

    val longTermModel = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(9))
      .setNumIterations(1)
      .setStepSize(0.0001)

    println("Continuous learning has started")

    longTermModel.trainOn(trainingData)

    longTermModel.predictOnValues(testingData.map(v => (v.label, v.features))).foreachRDD(
      rdd =>{
        if(!rdd.isEmpty()){
          rdd.foreach(point =>{
            println(s"Real: ${point._1} @ Predicted: ${point._2}")
          })
          println("MSE: %f".format(rdd.map(v => math.pow((v._1 - v._2), 2)).mean()))
          longTermModel.latestModel().toPMML(pmmlPath)
        }
      }
    )

    ssc2.start()
    ssc2.awaitTermination()
  }
}