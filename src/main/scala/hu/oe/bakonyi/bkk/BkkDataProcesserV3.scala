package hu.oe.bakonyi.bkk


import com.google.gson.Gson
import hu.oe.bakonyi.bkk.BkkDataProcesser.transformVector
import hu.oe.bakonyi.bkk.BkkDataProcesserV3.sparkSession
import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV4}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel, linalg}
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

object BkkDataProcesserV3 extends Serializable {

  System.setProperty("spark.driver.allowMultipleContexts", "true")

  val pipelineDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/pipeline"
  val modelDirectory =  "/mnt/D834B3AF34B38ECE/DEV/hadoop/model"
  val csvDirectory = "/mnt/D834B3AF34B38ECE/DEV/hadoop/bkk.csv"
  val zipPrefix = "jar:file:"
  val mleapPath = "/mnt/D834B3AF34B38ECE/DEV/hadoop//bkk-base-pipeline.zip"
  val asd = "D:\\DEV\\nodel"

  val dt = new DecisionTreeRegressor()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setImpurity("variance")
    .setMaxDepth(30)
    .setMaxBins(32)
    .setMinInstancesPerNode(5)

  val assembler = new VectorAssembler()
    .setInputCols(Array("routeId", "stopId", "month","dayOfWeek","hour","temperature","humidity","pressure","rain","snow","visibility"))
    .setOutputCol("features")

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val pipeline = new Pipeline()
  pipeline.setStages(Array(assembler,dt))
/*
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
  ).map(_.value)*/

  val sparkSession: SparkSession =  SparkSession.builder().config("spark.master", "local").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._

/*
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
        bkkv3Data.show(10, false)

        if(bkkv3Data.count() > 0) {

          val splits = bkkv3Data.randomSplit(Array(0.7, 0.3))
          val trainingData = splits(0)
          val testData = splits(1)

          val model: PipelineModel = pipeline.fit(trainingData)

          // Make predictions.
          val predictions: DataFrame = model.transform(testData)

          print(s"Predictions based on ${System.currentTimeMillis()} time train: ${System.lineSeparator()}")
          // Select example rows to display.
          predictions.show(50, false)

          // Select (prediction, true label) and compute test error
          val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
          val accuracy = evaluator.evaluate(predictions)

          println("Test Error = " + (1.0 - accuracy))

          pipeline.write.overwrite().save(pipelineDirectory)
          model.write.overwrite().save(modelDirectory)
          exportToMlLean(model,predictions,mleapPath)
        }else{
          print("No fitable values left after aggregating and preprocessing.")
        }
      }
    }
  )
*/



  def main(args: Array[String]): Unit = {
    //log.info("Spark JOB for BKK "+System.lineSeparator())
    //5ssc.start()

    val inputDf: DataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "bkk")
      .load()

    val routeData = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].map(x=>new Gson().fromJson(x._2,classOf[BkkBusinessDataV2]))
    //.withWatermark("lastUpdateTime", "5 minutes")

    val aggregatedRoutes = routeData
      .groupBy(
      //window($"lastUpdateTime", "10 minutes", "5 minutes"),
      $"month", $"dayOfWeek", $"lastUpdateTime", $"routeId", $"tripId", $"stopId", $"hour")
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
        ))

    val bkkv4Data: Dataset[BkkBusinessDataV4] = aggregatedRoutes.map(x =>{
      bkkv4Mapper(x)
    }).filter(x=> x.label > 0 && x.label < 1500000)


    BkkKafkaSender.sendKAfkaStuff(bkkv4Data)

    val query = bkkv4Data.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()

    query.awaitTermination()
  }

  def bkkv4Mapper(row:Row) : BkkBusinessDataV4 ={
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

object BkkKafkaSender extends Serializable{
  import sparkSession.implicits._

  def sendKAfkaStuff(bkkv4Data : Dataset[BkkBusinessDataV4]): Unit ={
    val learningKafkaDf = bkkv4Data.map(x => LabeledPoint(x.label, transformVector(x))).map(x => new Gson().toJson(x)).alias("value")
    learningKafkaDf.writeStream.format("kafka").outputMode(OutputMode.Complete()).option("checkpointLocation","C:\\Spark\\").option("kafka.bootstrap.servers","localhost:9092").option("topic","BKKLearningTopic").start()
   }
}
