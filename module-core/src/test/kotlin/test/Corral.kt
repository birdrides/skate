package test

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import net.postgis.jdbc.geometry.Point
import skate.TableName
import skate.Transient
import java.time.OffsetDateTime
import java.util.UUID

@TableName("corrals")
@JsonIgnoreProperties(ignoreUnknown = true)
data class Corral(
  val id: UUID = UUID.randomUUID(),
  val location: Point,
  val radius: Double,
  val maxQuantity: Int = 3,
  val address: String? = null,
  val photoIds: List<UUID> = listOf(),
  val notes: String? = null,
  val partnerId: UUID? = null,

  // Rider nests have hours of operation controlled by a config, but
  // certain other types of nests have specific hours of operation controlled by backend.
  val startHour: Int? = null,
  val endHour: Int? = null,
  @Transient val formattedOperationalHours: String? = null,

  val metadata: Map<String, Any> = mapOf(),
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val reviewedAt: OffsetDateTime? = null,
  @JsonIgnore
  val creatorId: UUID? = null,
  @JsonIgnore
  val reviewerId: UUID? = null,

  val riderParking: Boolean? = null,
  val riderParkingNotes: String? = null,

  @Deprecated("Use creator")
  @Transient val creatorDisplayName: String? = null,
  @Transient val creator: User? = null
)
