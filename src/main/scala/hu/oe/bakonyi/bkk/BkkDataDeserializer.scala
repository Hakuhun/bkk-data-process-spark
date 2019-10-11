package hu.oe.bakonyi.bkk

import java.util

import com.google.gson.Gson
import hu.oe.bakonyi.bkk.model.BkkBusinessData
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

class BkkDataDeserializer extends Deserializer[BkkBusinessData]{

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringDeserialiser = new StringDeserializer

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {
    stringDeserialiser.configure(map, b)
  }

  override def deserialize(s: String, bytes: Array[Byte]): BkkBusinessData = {
    val stringValue = stringDeserialiser.deserialize(s, bytes)
    new Gson().fromJson(stringValue, classOf[BkkBusinessData])
  }

  override def close(): Unit = {
    stringDeserialiser.close()
  }
}
