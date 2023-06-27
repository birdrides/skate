package test

import net.postgis.jdbc.geometry.Point
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("vehicle_tracks")
data class VehicleTrack(
  val id: UUID = UUID.randomUUID(),
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val speed: Double? = null,
  val heading: Double? = null,
  val location: Point? = null,
  val batteryLevel: Int? = null,
  val locked: Boolean? = null,
  val distance: Int? = null,
  val vehicleId: UUID = UUID.randomUUID()
)
