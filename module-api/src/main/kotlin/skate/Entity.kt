package skate

import java.time.OffsetDateTime
import java.util.UUID

interface Entity {
  val id: UUID
}

interface Deletable {
  val deletedAt: OffsetDateTime?
}
