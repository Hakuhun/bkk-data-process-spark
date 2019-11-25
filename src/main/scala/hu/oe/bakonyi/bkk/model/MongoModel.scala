package hu.oe.bakonyi.bkk.model

import org.bson.types.ObjectId
import com.sfxcode.nosql.mongo.MongoDAO
import com.sfxcode.nosql.mongo.database.DatabaseProvider
import org.bson.codecs.configuration.CodecRegistries._
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.Macros._

object MongoModel {
  case class MongoModel(time: Time, route: Route, weather: Weather, label: Double, _id: ObjectId = new ObjectId())

  case class Time(month: Int, dayOfWeek: Int, hour: Int)

  case class Route(routeId: Double, stopId: Double, alertt: Boolean)

  case class Weather(temperature: Double, humidity: Double, pressure: Double, visibility: Double)

  private val registry = fromProviders(classOf[MongoModel], classOf[Time], classOf[Route], classOf[Weather])

  val provider = DatabaseProvider("routes-backup", registry)

  object MongoModelDAO extends MongoDAO[MongoModel](provider, "routes")
}