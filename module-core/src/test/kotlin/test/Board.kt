package test

import skate.Entity
import skate.TableName
import skate.Transient
import java.time.OffsetDateTime
import java.util.UUID

@TableName("boards")
data class Board(
  override val id: UUID = UUID.randomUUID(),
  val phone: String? = null,
  val imei: String,
  val iccid: String? = null,
  val vehicleId: UUID? = null,
  val protocol: String,
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime = OffsetDateTime.now(),
  val model: String? = null,
  val firmwareVersion: String? = null,
  val disabled: Boolean = false,

  @Transient val vehicle: Vehicle? = null
) : Entity
