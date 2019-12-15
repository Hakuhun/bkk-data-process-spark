package hu.oe.bakonyi.bkk.model

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class BkkBusinessDataV2(
                              month: Int,
                              dayOfWeek: Int,
                              hour: Int,
                              lastUpdateTime: Timestamp,
                              routeId: String,
                              tripId: String,
                              stopId: String,
                              vehicleModel: String,
                              temperature: Double,
                              humidity: Double,
                              pressure: Double,
                              snow: Double,
                              rain: Double,
                              visibility: Double,
                              departureDiff: Double,
                              arrivalDiff: Double,
                              alert: Byte,
                              value: Double,
                            )

case class BkkBusinessDataV3(
                              month: Int,
                              dayOfWeek: Int,
                              hour: Int,
                              //lastUpdateTime: Long,
                              routeId: Int,
                              //tripId : String,
                              //stopId : String,
                              //vehicleModel : String,
                              temperature: Double,
                              humidity: Double,
                              pressure: Double,
                              snow: Double,
                              rain: Double,
                              visibility: Double,
                              //departureDiff : Double,
                              //arrivalDiff: Double,
                              alert: Byte,
                              value: Double,
                            )

case class BkkBusinessDataV4(
                              month: Int,
                              dayOfWeek: Int,
                              hour: Int,
                              //lastUpdateTime: Long,
                              routeId: Int,
                              //tripId : String,
                              stopId: Int,
                              //vehicleModel : String,
                              temperature: Double,
                              humidity: Double,
                              pressure: Double,
                              snow: Double,
                              rain: Double,
                              visibility: Double,
                              //departureDiff : Double,
                              //arrivalDiff: Double,
                              alert: Byte,
                              label: Double,
                            ) {
  def this() = this(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
}
