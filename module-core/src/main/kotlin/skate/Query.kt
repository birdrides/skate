package skate

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

data class Query(
  val projections: List<Projection>,
  val aggregates: List<Aggregate>,
  val fromClauses: List<From>,
  val whereClause: Expression<Boolean>? = null,
  val orderClauses: List<Order>? = null,
  val limitNumber: Int? = null,
  val offsetNumber: Int? = null,
  val grouped: Boolean = false,
  val distinct: Boolean = false,
  val random: Boolean = false,
  val distinctOnClauses: List<Projection>? = null
)

data class Projection(
  val expression: Expression<*>,
  val alias: String? = null
)

data class Aggregate(
  val expression: AggregateFunction<*, *>,
  val alias: String? = null,
  val unnested: Boolean = false
)

data class AggregateFunction<S, R>(
  val name: String,
  val expression: Expression<S>,
  val arguments: List<Value<*>> = listOf()
)

enum class NullsOrderPref {
  NULLS_FIRST, NULLS_LAST
}

sealed class Order
data class ExpressionOrder(
  val expression: Expression<*>,
  val descending: Boolean = false,
  val nullsPref: NullsOrderPref? = null
) : Order()

data class CaseOrder(
  val cases: List<WhenThen<Int>>
) : Order()

sealed class From
data class Table<T : Any>(val type: KClass<T>, val alias: String? = null) : From()
data class Join(
  val query: Query,
  val kind: JoinKind? = null,
  val intoField: IntoField<*, *>? = null,
  val on: Expression<Boolean>? = null,
  val using: List<Column<*, *>>? = null,
  val alias: String? = null
) : From()

data class SubQuery(val query: Query, val alias: String? = null) : From()

data class IntoField<T : Any, R : Any>(val type: KClass<R>, val column: Column<T, R?>)

enum class JoinKind { INNER, LEFT, RIGHT, FULL; }

data class TableAlias<T : Any>(val type: KClass<T>, val alias: String) {
  operator fun <R> get(prop: KProperty1<T, R?>): Column<T, R?> = Column(prop, Table(type, alias))
}

fun <T : Any> literal(value: T?): Expression<T?> = Value(value)
