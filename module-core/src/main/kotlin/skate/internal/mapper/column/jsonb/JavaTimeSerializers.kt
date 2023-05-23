package skate.internal.mapper.column.jsonb

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import java.io.IOException
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

private object Formatter {
  val OFFSET: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  val LOCAL: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
}

class OffsetDateTimeSerializer : JsonSerializer<OffsetDateTime>() {
  @Throws(IOException::class, JsonProcessingException::class)
  override fun serialize(value: OffsetDateTime, generator: JsonGenerator, serializers: SerializerProvider) {
    generator.writeString(Formatter.OFFSET.format(value))
  }
}

class OffsetDateTimeDeserializer : JsonDeserializer<OffsetDateTime>() {
  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(parser: JsonParser, context: DeserializationContext): OffsetDateTime? {
    val text = parser.readNode().asText()
    return text?.let { Formatter.OFFSET.parse(text, OffsetDateTime::from) }
  }
}

class LocalDateTimeSerializer : JsonSerializer<LocalDateTime>() {
  @Throws(IOException::class, JsonProcessingException::class)
  override fun serialize(value: LocalDateTime, generator: JsonGenerator, serializers: SerializerProvider) {
    generator.writeString(Formatter.LOCAL.format(value))
  }
}

class LocalDateTimeDeserializer : JsonDeserializer<LocalDateTime>() {
  @Throws(IOException::class, JsonProcessingException::class)
  override fun deserialize(parser: JsonParser, context: DeserializationContext): LocalDateTime? {
    val text = parser.readNode().asText()
    return text?.let { Formatter.LOCAL.parse(text, LocalDateTime::from) }
  }
}

class ChronoUnitSerializer : JsonSerializer<ChronoUnit>() {
  @Throws(IOException::class, JsonProcessingException::class)
  override fun serialize(value: ChronoUnit, generator: JsonGenerator, serializers: SerializerProvider) {
    generator.writeString(value.name.toLowerCase())
  }
}

class ChronoUnitDeserializer : JsonDeserializer<ChronoUnit>() {
  @Throws(IOException::class, JsonProcessingException::class, IllegalArgumentException::class)
  override fun deserialize(parser: JsonParser, context: DeserializationContext): ChronoUnit? {
    val text = parser.readNode().asText()
    return text?.let { ChronoUnit.valueOf(it) }
  }
}

internal fun JsonParser.readNode(): JsonNode {
  return codec.readTree(this)
}
