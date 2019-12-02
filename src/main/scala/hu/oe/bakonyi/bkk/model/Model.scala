package hu.oe.bakonyi.bkk.model

import org.apache.spark.mllib

case class Location(lat : Double, lon: Double)
case class BkkData(location: Location, routeId:String, tripId:String, veichleId: String, stopId:String, lastUpdateTime:Long, stopSequence:Integer, departureTime: Long, estimatedDepartureTime:Long, arrivalTime:Long,estimatedArrivalTime:Long, departureDiff:Long, arrivalDiff:String, model:String)
case class BasicWeatherModel(temperature:Double, feelsLikeTemperature:Double, humidity:Double, pressure:Double, rainIntensity:Double, snowIntensity:Double, visibility:Integer, location: Location)
case class BkkBusinessData(location: Location, bkk:BkkData, weather: BasicWeatherModel, currentTime:Long, month:Integer, dayOfTheWeek: Integer)

case class  Features(values: Array[Double])

case class MLReadyBkkModel(
                          label:Double,
                          features: Features
                          )