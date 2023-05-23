package test

import org.postgis.Point
import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("tasks")
data class Task(
  override val id: UUID = UUID.randomUUID(),
  val vehicleId: UUID,
  val price: Int = 0,
  val adminId: UUID? = null,
  val location: Point? = null,
  val zoneId: UUID? = null,
  val taskId: UUID? = null,
  val userId: UUID? = null,
  val kind: TaskKind = TaskKind.BATTERY,

  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime = OffsetDateTime.now(),
  val openedAt: OffsetDateTime = OffsetDateTime.now(),
  val closedAt: OffsetDateTime? = null,
) : Entity

enum class TaskKind {
  LOCATION,
  OFFLINE,
  BATTERY,
  MISSING,
  SERVICE;
}
