package test

import skate.Entity
import skate.TableName
import java.time.OffsetDateTime
import java.util.UUID

@TableName("invites")
data class Invite(
  override val id: UUID = UUID.randomUUID(),
  val userId: UUID,
  val identifier: String,
  val firstName: String? = null,
  val lastName: String? = null,
  val phones: List<String> = listOf(),
  val emails: List<String> = listOf(),
  val zipCodes: List<String> = listOf(),
  val sentTo: List<String> = listOf(),
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val sentAt: OffsetDateTime? = null
) : Entity
