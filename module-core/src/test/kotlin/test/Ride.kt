package test

import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("rides")
data class Ride(
  override val id: UUID = UUID.randomUUID(),
  val userId: UUID = UUID.randomUUID(),
  val vehicleId: UUID = UUID.randomUUID(),
  val startedAt: OffsetDateTime = OffsetDateTime.now(),
  val unlockedAt: OffsetDateTime? = null,
  val canceledAt: OffsetDateTime? = null,
  val completedAt: OffsetDateTime? = null,
  val initialDistance: Int = 0,
  val currentDistance: Int = 0,
  val initialBattery: Int = 0,
  val currentBattery: Int = 0,
  val cost: Int? = null,
  val endPhotoUrl: String? = null,
  val zoneId: UUID? = null,
  val partnerId: UUID? = null
) : Entity
