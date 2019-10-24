package hu.oe.bakonyi.bkk

import java.util

import com.google.gson.Gson
import hu.oe.bakonyi.bkk.model.BkkBusinessDataV2
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
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
