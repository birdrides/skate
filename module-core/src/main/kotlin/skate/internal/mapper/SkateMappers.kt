@file:Suppress("UNCHECKED_CAST")

package skate.internal.mapper

import org.jdbi.v3.core.config.ConfigRegistry
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.mapper.RowMapperFactory
import skate.EmptyUpdateFrom
import skate.Join
import skate.Query
import skate.Update
import java.lang.reflect.Type
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

object SkateMappers : RowMapperFactory {

  private val cache = ConcurrentHashMap<KClass<*>, SkateMapper<*>>()

  fun <T : Any> resolve(type: KClass<T>, query: Query?): SkateMapper<T> {
    // For update join query, ignore cache
    if (query != null && query.fromClauses.any { it is Join && it.intoField != null }) {
      return SkateMapper.join(type, query)
    }

    return resolve(type)
  }

  fun <T : Any> resolve(type: KClass<T>, update: Update<*>?): SkateMapper<T> {
    // For join query, ignore cache
    if (update != null && update.fromClause != EmptyUpdateFrom && update.intoFields.isNotEmpty()) {
      return SkateMapper.join(type, update)
    }

    return resolve(type)
  }

  private fun <T : Any> resolve(type: KClass<T>): SkateMapper<T> {
    // ConcurrentHashMap can enter an infinite loop on nested computeIfAbsent calls.
    // Since row mappers can decorate other row mappers, we have to populate the cache the old fashioned way.
    // See https://bugs.openjdk.java.net/browse/JDK-8062841, https://bugs.openjdk.java.net/browse/JDK-8142175
    val cached = cache[type]
    if (cached != null) {
      return cached as SkateMapper<T>
    }

    // If type is enum, we must use enum mapper because enum class doesn't have constructor
    val mapper = if (type.java.isEnum) {
      SkateMapper.enum(type)
    } else {
      SkateMapper.default(type)
    }
    cache[type] = mapper
    return mapper
  }

  /**
   * Eventually we want to register this with JDBI instead of using a singleton instance of [SkateMappers]
   */
  override fun build(type: Type, config: ConfigRegistry?): Optional<RowMapper<*>> {
    return Optional.empty()
  }
}
