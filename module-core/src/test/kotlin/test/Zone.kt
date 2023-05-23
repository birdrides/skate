package test

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.postgis.Geometry
import org.postgis.Point
import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("zones")
@JsonIgnoreProperties(ignoreUnknown = true)
data class Zone(
  override val id: UUID = UUID.randomUUID(),
  val name: String = "default",
  val description: String? = null,
  val region: Geometry? = null,
  val centerPoint: Point? = null,
  val timeZone: String? = "America/Los_Angeles",
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime? = null,
  val deletedAt: OffsetDateTime? = null,
) : Entity
