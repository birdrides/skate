package skate.internal.mapper

import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.mapper.reflect.ColumnNameMatcher
import skate.Query
import skate.Update
import kotlin.reflect.KClass

interface SkateMapper<T : Any> : RowMapper<T> {
  /**
   * Return true if any [ColumnNameMatcher] matches column name and parameter name
   */
  fun List<ColumnNameMatcher>.matches(columnName: String, parameterName: String?): Boolean {
    return any {
      it.columnNameMatches(columnName, parameterName)
    }
  }

  companion object {

    /**
     * Cacheable default [SkateMapper]
     *
     * @param type An entity class of type T
     */
    fun <T : Any> default(type: KClass<T>): SkateMapper<T> {
      return DefaultSkateMapper(type)
    }

    /**
     * Cacheable enum [SkateMapper]
     *
     * @param type An enum class of type T
     */
    fun <T : Any> enum(type: KClass<T>): SkateMapper<T> {
      return EnumSkateMapper(type)
    }

    /**
     * Join [SkateMapper] with raw query
     *
     * @param type An entity class of type T
     * @param query A raw query SQL
     */
    fun <T : Any> join(type: KClass<T>, query: Query): SkateMapper<T> {
      return JoinSkateMapper(type, query)
    }

    /**
     * Join [SkateMapper] with Update statement
     *
     * @param type An entity class of type T
     * @param update A raw update SQL
     */
    fun <T : Any> join(type: KClass<T>, update: Update<*>): SkateMapper<T> {
      return JoinUpdateSkateMapper(type, update)
    }
  }
}
