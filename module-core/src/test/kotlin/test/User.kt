package test

import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("users")
data class User(
  override val id: UUID = UUID.randomUUID(),
  val name: String? = null,
  val email: String? = null,
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime? = null
) : Entity
