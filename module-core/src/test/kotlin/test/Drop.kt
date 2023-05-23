package test

import org.postgis.Point
import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("drops")
data class Drop(
  override val id: UUID = UUID.randomUUID(),
  val userId: UUID? = null,
  val nestId: UUID,
  val releaseLocation: Point? = null,
  val quantity: Int = 3,
  val photoUrl: String? = null,
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val releasedAt: OffsetDateTime? = null,
  val expireAt: OffsetDateTime? = null,
  val claimedAt: OffsetDateTime? = null,
  val unclaimAt: OffsetDateTime? = null,
  val updatedAt: OffsetDateTime? = OffsetDateTime.now(),
  val metadata: Map<String, Any> = mapOf(),
  val location: Point?,
) : Entity
