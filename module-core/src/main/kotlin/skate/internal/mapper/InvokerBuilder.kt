@file:Suppress("UNCHECKED_CAST")

package skate.internal.mapper

import org.jdbi.v3.core.mapper.reflect.ColumnNameMatcher
import org.jdbi.v3.core.mapper.reflect.ReflectionMappers
import org.jdbi.v3.core.statement.StatementContext
import skate.IntoField
import skate.Join
import skate.Query
import skate.Update
import skate.generator.joinEnd
import skate.generator.joinStart
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.full.memberProperties

internal object InvokerBuilder {

  fun <U : Any> build(
    type: KClass<U>,
    update: Update<*>?,
    columns: List<Column>,
    context: StatementContext
  ): Invoker<U> {
    val mutableColumns = columns.toMutableList()
    // Capture transient properties so we can exclude them from constructor
    val transientPropertyNames = type
      .memberProperties
      .filter { p -> p.annotations.any { it is Transient } }
      .map { it.name }
      .toMutableSet()

    // Remove all join columns and create value providers for each join.
    val joinValues = mutableListOf<Pair<String, ValueProvider>>()
    update?.let {
      it.intoFields.forEach { intoField ->
        val joinColumns = removeJoinColumns(mutableColumns, intoField)

        val columnName = intoField.column.property.name
        val builder = build(type = intoField.type, update = null, columns = joinColumns, context = context)
        joinValues.add(Pair(columnName, ValueProvider.join(builder, joinColumns)))
        // Join column is @Transient but its value is provided by join clause so exclude them here
        transientPropertyNames.remove(columnName)
      }
    }

    return invoker(
      type = type,
      transientPropertyNames = transientPropertyNames,
      columns = mutableColumns,
      joinValues = joinValues,
      context = context
    )
  }

  fun <U : Any> build(
    type: KClass<U>,
    query: Query,
    columns: List<Column>,
    context: StatementContext
  ): Invoker<U> {
    val mutableColumns = columns.toMutableList()
    // Capture transient properties so we can exclude them from constructor
    val transientPropertyNames = type
      .memberProperties
      .filter { p -> p.annotations.any { it is Transient } }
      .map { it.name }
      .toMutableSet()

    // Remove all join columns and create value providers for each join.
    val joinValues = mutableListOf<Pair<String, ValueProvider>>()
    for (from in query.fromClauses) {
      if (from is Join && from.intoField != null) {
        val joinColumns = removeJoinColumns(mutableColumns, from.intoField)
        val joinQuery = from.query
        val joinTable = from.query.fromClauses.firstOrNull()

        if (joinTable != null) {
          val columnName = from.intoField.column.property.name
          val builder = build(type = from.intoField.type, query = joinQuery, columns = joinColumns, context = context)
          joinValues.add(Pair(columnName, ValueProvider.join(builder, joinColumns)))
          // Join column is @Transient but its value is provided by join clause so exclude them here
          transientPropertyNames.remove(columnName)
        }
      }
    }

    return invoker(
      type = type,
      transientPropertyNames = transientPropertyNames,
      columns = mutableColumns,
      joinValues = joinValues,
      context = context
    )
  }

  private fun <U : Any> invoker(
    type: KClass<U>,
    transientPropertyNames: Set<String>,
    columns: List<Column>,
    joinValues: List<Pair<String, ValueProvider>>,
    context: StatementContext
  ): Invoker<U> {
    val constructor = type.findConstructor()
    val parameters = mutableMapOf<KParameter, ValueProvider>()
    val properties = mutableListOf<Pair<KMutableProperty1<U, *>, ValueProvider>>()
    val constructorParameters = constructor.parameters
    val mutableProperties = type.memberProperties.mapNotNull { it as? KMutableProperty1 }

    val columnNameMatchers = context.getConfig(ReflectionMappers::class.java).columnNameMatchers
    val slots = hashMapOf<String, Slot>()
    for (property in mutableProperties) {
      slots[property.propertyName()] = Slot.MutableProperty(property)
    }

    for (parameter in constructorParameters) {
      val parameterName = parameter.parameterName() ?: continue
      // Note that we only want to store non-transient parameters for value providers because
      // we can't capture non-nullable parameter default value at reflection time and it's null from
      // database. To workaround this issue, we need to rely on Kotlin's generated constructor
      // with default values and let it take over when construct entity
      if (parameterName !in transientPropertyNames) {
        slots[parameterName] = Slot.Parameter(parameter)
      }
    }

    for ((slotName, slot) in slots) {
      val (columnIdx, _) = columns.find { (_, name) -> columnNameMatchers.matches(name, slotName) } ?: continue
      when (slot) {
        is Slot.MutableProperty<*> -> {
          properties.add(
            Pair(
              slot.property as KMutableProperty1<U, *>,
              ValueProvider.column(slot.property.returnType, columnIdx, context)
            )
          )
        }

        is Slot.Parameter -> {
          parameters[slot.parameter] = ValueProvider.column(slot.parameter.type, columnIdx, context)
        }
      }
    }

    // Add value providers for each join.
    for ((columnName, valueProvider) in joinValues) {
      val parameter = constructorParameters.find { columnNameMatchers.matches(columnName, it.parameterName()) }
      if (parameter != null) {
        parameters[parameter] = valueProvider
      }
    }

    // Fill in required nullable parameters that are missing.
    for (parameter in constructorParameters) {
      if (!parameter.isOptional && parameter.type.isMarkedNullable) {
        parameters.putIfAbsent(parameter, ValueProvider.NULL)
      }
    }

    return Invoker(type, constructor, parameters, properties)
  }

  private fun removeJoinColumns(columns: MutableList<Column>, intoField: IntoField<*, *>): List<Column> {
    val startIndex = columns.indexOfFirst { it.second == intoField.joinStart() }
    val endIndex = columns.indexOfFirst { it.second == intoField.joinEnd() }

    val joinColumns = mutableListOf<Column>()
    for (index in startIndex..endIndex) {
      val element = columns.removeAt(startIndex)
      if (index != startIndex && index != endIndex) {
        joinColumns.add(element)
      }
    }
    return joinColumns
  }

  /**
   * Return true if any [ColumnNameMatcher] matches column name and parameter name
   */
  private fun List<ColumnNameMatcher>.matches(columnName: String, parameterName: String?): Boolean {
    return any {
      it.columnNameMatches(columnName, parameterName)
    }
  }
}
