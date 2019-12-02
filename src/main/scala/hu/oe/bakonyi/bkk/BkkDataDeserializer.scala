package hu.oe.bakonyi.bkk

import java.util

import com.google.gson.Gson
import hu.oe.bakonyi.bkk.model.{BkkBusinessDataV2, BkkBusinessDataV4, MLReadyBkkModel}
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.apache.spark.mllib.regression.LabeledPoint
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

class BkkDataDeserializer extends Deserializer[BkkBusinessDataV2]{

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringDeserialiser = new StringDeserializer

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
    stringDeserialiser.configure(map, b)
  }

  override def deserialize(s: String, bytes: Array[Byte]): BkkBusinessDataV2 = {
    val stringValue = stringDeserialiser.deserialize(s, bytes)
    new Gson().fromJson(stringValue, classOf[BkkBusinessDataV2])
  }

  override def close(): Unit = {
    stringDeserialiser.close()
  }
}

class RefinedBkkDeserializer extends Serializer[BkkBusinessDataV4]{

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringDeserialiser = new StringSerializer

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
    stringDeserialiser.configure(map, b)
  }
  override def close(): Unit = {
    stringDeserialiser.close()
  }

  override def serialize(topic: String, data: BkkBusinessDataV4): Array[Byte] = {
    var line =""
    line = new Gson().toJson(data)
    line.getBytes("UTF-8")
  }
}

class MlReadyBkkModelDeserializator extends Deserializer[LabeledPoint]{

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringDeserialiser = new StringDeserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    stringDeserialiser.configure(configs, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): LabeledPoint = {
    val stringValue = stringDeserialiser.deserialize(topic, data)
    val json = new Gson().fromJson(stringValue, classOf[MLReadyBkkModel])

    val features = org.apache.spark.mllib.linalg.Vectors.dense(json.features.values)

    LabeledPoint(json.label,features)
  }

  override def close(): Unit = {
    stringDeserialiser.close()
  }
}