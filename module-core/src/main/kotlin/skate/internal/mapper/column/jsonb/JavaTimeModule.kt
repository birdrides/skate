package skate.internal.mapper.column.jsonb

import com.fasterxml.jackson.databind.module.SimpleModule
import java.time.LocalDateTime
import java.time.OffsetDateTime

class JavaTimeModule : SimpleModule() {
  init {
    addSerializer(OffsetDateTime::class.java, OffsetDateTimeSerializer())
    addDeserializer(OffsetDateTime::class.java, OffsetDateTimeDeserializer())
    addSerializer(LocalDateTime::class.java, LocalDateTimeSerializer())
    addDeserializer(LocalDateTime::class.java, LocalDateTimeDeserializer())
  }
}
