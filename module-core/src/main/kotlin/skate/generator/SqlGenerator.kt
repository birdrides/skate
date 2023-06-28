package skate.generator

import skate.ArrayComparisonOperator
import skate.Column
import skate.ColumnName
import skate.ComparisonOperator
import skate.DateOperator
import skate.Factory
import skate.IntoField
import skate.JoinKind
import skate.JsonBOperator
import skate.NumericOperator
import skate.Table
import skate.TableName
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaField

internal interface SqlGenerator<E> {
  /**
   * Generate a SQL expression for an instance of type [E]
   *
   * @param e An instance of type [E] where E can be expressions, operators or queries
   */
  operator fun invoke(e: E): String

  companion object {

    internal val NUMERIC_OPERATOR = object : SqlGenerator<NumericOperator> {
      override fun invoke(e: NumericOperator): String {
        return when (e) {
          NumericOperator.PLUS -> "+"
          NumericOperator.MINUS -> "-"
          NumericOperator.TIMES -> "*"
          NumericOperator.DIV -> "/"
          NumericOperator.MOD -> "%"
          NumericOperator.POW -> "^"
        }
      }
    }

    internal val DATE_OPERATOR = object : SqlGenerator<DateOperator> {
      override fun invoke(e: DateOperator): String {
        return when (e) {
          DateOperator.MINUS_INTERVAL -> "-"
          DateOperator.PLUS_INTERVAL -> "+"
        }
      }
    }

    internal val COMPARISON_OPERATOR = object : SqlGenerator<ComparisonOperator> {
      override fun invoke(e: ComparisonOperator): String {
        return when (e) {
          ComparisonOperator.EQ -> "="
          ComparisonOperator.NE -> "!="
          ComparisonOperator.GT -> ">"
          ComparisonOperator.GTE -> ">="
          ComparisonOperator.LT -> "<"
          ComparisonOperator.LTE -> "<="
        }
      }
    }

    internal val JSONB = object : SqlGenerator<JsonBOperator> {
      override fun invoke(e: JsonBOperator): String {
        return when (e) {
          JsonBOperator.GET -> " -> "
          JsonBOperator.GET_AS_TEXT -> " ->> "
        }
      }
    }

    internal val ARRAY_COMPARISON_OPERATOR = object : SqlGenerator<ArrayComparisonOperator> {
      override fun invoke(e: ArrayComparisonOperator): String {
        return when (e) {
          ArrayComparisonOperator.CONTAINS -> "@>"
          ArrayComparisonOperator.CONTAINED_BY -> "<@"
          ArrayComparisonOperator.OVERLAPS -> "&&"
        }
      }
    }

    internal val JOIN_KIND = object : SqlGenerator<JoinKind> {
      override fun invoke(e: JoinKind): String {
        return when (e) {
          JoinKind.INNER -> "JOIN"
          JoinKind.LEFT -> "LEFT JOIN"
          JoinKind.RIGHT -> "RIGHT JOIN"
          JoinKind.FULL -> "FULL JOIN"
        }
      }
    }

    internal val COLUMN = CacheableSqlGenerator.build(
      generator = object : SqlGenerator<Column<*, *>> {
        override fun invoke(e: Column<*, *>): String {
          return "\"${e.name()}\""
        }
      },
      key = { column ->
        "${column.table.type.qualifiedName}|${column.property.name}"
      }
    )

    internal val JOIN_START = CacheableSqlGenerator.build(
      generator = object : SqlGenerator<IntoField<*, *>> {
        override fun invoke(e: IntoField<*, *>): String {
          return "start:${e.column.name()}"
        }
      },
      key = { field ->
        "${field.column.table.type.qualifiedName}|${field.column.property.name}"
      }
    )

    internal val JOIN_END = CacheableSqlGenerator.build(
      generator = object : SqlGenerator<IntoField<*, *>> {
        override fun invoke(e: IntoField<*, *>): String {
          return "end:${e.column.name()}"
        }
      },
      key = { field ->
        "${field.column.table.type.qualifiedName}|${field.column.property.name}"
      }
    )

    internal val TABLE = CacheableSqlGenerator.build(
      generator = object : SqlGenerator<Table<*>> {
        override fun invoke(e: Table<*>): String {
          val annotation = e.type.findAnnotation<TableName>()
          val name = annotation?.name
          return name ?: e.type.simpleName?.toUnderscore()
            ?: throw IllegalArgumentException("Entity class must have name!")
        }
      },
      key = { table ->
        table.type.qualifiedName ?: throw IllegalArgumentException("Entity class must have name!")
      }
    )
  }
}

private fun Column<*, *>.name(): String {
  val constructorParameters = findConstructor(table.type).parameters
  val matchingParameter = constructorParameters.find { it.name == property.name }
  val columnNameAnnotation = matchingParameter?.findAnnotation()
    ?: property.findAnnotation()
    ?: property.getter.findAnnotation()
    ?: property.javaField?.getAnnotation(ColumnName::class.java)
  return columnNameAnnotation?.value ?: property.name.toUnderscore()
}

private fun <C : Any> findConstructor(type: KClass<C>): KFunction<C> {
  return when {
    type.isSealed && type.companionObject?.java?.interfaces?.contains(Factory::class.java) == true -> {
      @Suppress("UNCHECKED_CAST")
      val c = type.companionObjectInstance as Factory<C>
      c.factoryFunction
    }

    type.primaryConstructor != null ->
      type.primaryConstructor
        ?: throw IllegalArgumentException("A bean, ${type.simpleName} was mapped which was not instantiable (cannot find appropriate constructor)")

    type.constructors.size == 1 -> type.constructors.first()
    else -> throw IllegalArgumentException("A bean, ${type.simpleName} was mapped which was not instantiable (cannot find appropriate constructor)")
  }
}
