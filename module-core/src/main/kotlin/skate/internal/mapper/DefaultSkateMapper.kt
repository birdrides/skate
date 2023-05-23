@file:Suppress("UNCHECKED_CAST")

package skate.internal.mapper

import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.mapper.reflect.ReflectionMappers
import org.jdbi.v3.core.statement.StatementContext
import java.sql.ResultSet
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.full.memberProperties

internal class DefaultSkateMapper<T : Any>(
  private val type: KClass<T>
) : SkateMapper<T> {

  private val constructor = type.findConstructor()
  private val constructorParameters = constructor.parameters
  private val mutableProperties = type.memberProperties.mapNotNull { it as? KMutableProperty1 }
  private val nullableParameters = constructorParameters.filter { !it.isOptional && it.type.isMarkedNullable }
  private val slotCache = ConcurrentHashMap<String, Slot?>()

  @Throws(SQLException::class)
  override fun map(rs: ResultSet, ctx: StatementContext): T {
    return specialize(rs, ctx).map(rs, ctx)
  }

  @Throws(SQLException::class)
  override fun specialize(resultSet: ResultSet, context: StatementContext): RowMapper<T> {
    val metadata = resultSet.metaData
    // From clause can cause duplicate columns, we want to the order from smallest to largest because
    // the result is the entity from select clause
    val columns = (1..metadata.columnCount)
      .map { Pair(it, metadata.getColumnLabel(it)) }
      .distinctBy { (_, name) -> name.toLowerCase() }

    val invoker = invoker(type, constructor, columns, context)
    return RowMapper { r, c -> invoker(r, c) }
  }

  @Suppress("UNCHECKED_CAST")
  private fun <U : Any> invoker(
    type: KClass<U>,
    constructor: KFunction<U>,
    columns: List<Column>,
    context: StatementContext
  ): Invoker<U> {
    val parameters = mutableMapOf<KParameter, ValueProvider>()
    val properties = mutableListOf<Pair<KMutableProperty1<U, *>, ValueProvider>>()
    val columnNameMatchers = context.getConfig(ReflectionMappers::class.java).columnNameMatchers
    for ((columnNumber, columnName) in columns) {
      val slot = slotCache.computeIfAbsent(columnName) {
        val parameter = constructorParameters.find { columnNameMatchers.matches(columnName, it.parameterName()) }
        if (parameter != null) {
          Slot.Parameter(parameter)
        } else {
          val property = mutableProperties.find { columnNameMatchers.matches(columnName, it.propertyName()) }
          if (property != null) {
            Slot.MutableProperty(property)
          } else {
            null
          }
        }
      }

      if (slot != null) {
        when (slot) {
          is Slot.MutableProperty<*> -> {
            properties.add(
              Pair(
                slot.property as KMutableProperty1<U, *>,
                ValueProvider.column(slot.property.returnType, columnNumber, context)
              )
            )
          }

          is Slot.Parameter -> {
            parameters[slot.parameter] = ValueProvider.column(slot.parameter.type, columnNumber, context)
          }
        }
      }
    }

    nullableParameters.forEach {
      parameters.putIfAbsent(it, ValueProvider.NULL)
    }
    return Invoker(type, constructor, parameters, properties)
  }
}
