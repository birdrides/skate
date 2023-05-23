package test

import org.postgis.Point
import skate.TableName
import skate.Transient
import java.time.OffsetDateTime
import java.util.UUID

@TableName("vehicles")
data class Vehicle(
  val id: UUID = UUID.randomUUID(),
  val code: String = "",
  val location: Point? = null,
  val serialNumber: String? = null,
  val zoneId: UUID? = null,
  val cellular: Boolean = true,
  val heading: Double = 0.0,
  val distance: Int = 0,
  val rideId: UUID? = null,
  val taskId: UUID? = null,
  val userId: UUID? = null,
  val batteryLevel: Int = 0,
  val estimatedRange: Int? = null,
  val locked: Boolean = true,
  val tags: List<String>? = null,
  val model: String? = "m365",
  val pattern: String? = null,

  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime = OffsetDateTime.now(),
  val releasedAt: OffsetDateTime? = null,
  val trackedAt: OffsetDateTime? = null,

  @Transient val task: Task? = null,
  @Transient val board: Board? = null
)
