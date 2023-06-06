@file:Suppress("unused", "UNCHECKED_CAST")

package skate

import org.postgresql.util.PGInterval
import skate.ArrayComparisonOperator.CONTAINED_BY
import skate.ArrayComparisonOperator.CONTAINS
import skate.ArrayComparisonOperator.OVERLAPS
import skate.ComparisonOperator.EQ
import skate.ComparisonOperator.GT
import skate.ComparisonOperator.GTE
import skate.ComparisonOperator.LT
import skate.ComparisonOperator.LTE
import skate.ComparisonOperator.NE
import skate.DateOperator.MINUS_INTERVAL
import skate.DateOperator.PLUS_INTERVAL
import skate.JoinKind.FULL
import skate.JoinKind.INNER
import skate.JoinKind.LEFT
import skate.JoinKind.RIGHT
import skate.NumericOperator.DIV
import skate.NumericOperator.MINUS
import skate.NumericOperator.MOD
import skate.NumericOperator.PLUS
import skate.NumericOperator.POW
import skate.NumericOperator.TIMES
import skate.generator.Dialect
import java.time.OffsetDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.jvmName

inline fun <reified T : Any, R> KProperty1<T, R?>.column() = Column(this, Table(T::class))
inline fun <reified T : Any, R> KProperty1<T, R?>.columnAs(alias: String) = Column(this, Table(T::class, alias))
inline fun <reified T : Any, R> KProperty1<T, R?>.project() = Projection(this.column(), null)
inline fun <reified T : Any, R> KProperty1<T, R?>.projectAs(alias: String) = Projection(this.column(), alias)
fun <R> Expression<R>.projectAs(alias: String) = Projection(this, alias)

inline fun <reified R : Any> R.value() = Value(this)
inline fun <reified R : Any> List<R>.array() = Array(this, R::class)

// Comparison
// TODO: Think about whether all eq/ne variants should allow optional R type parameters.
inline infix fun <reified T : Any, R> KProperty1<T, R>.eq(value: R) where R : Any, R : Comparable<R> =
  Comparison(EQ, this.column(), Value(value))

inline infix fun <reified T : Any, R> KProperty1<T, R>.ne(value: R) where R : Any, R : Comparable<R> =
  Comparison(NE, this.column(), Value(value))

inline infix fun <reified T : Any, R> KProperty1<T, R>.gt(value: R) where R : Any, R : Comparable<R> =
  Comparison(GT, this.column(), Value(value))

inline infix fun <reified T : Any, R> KProperty1<T, R>.gte(value: R) where R : Any, R : Comparable<R> =
  Comparison(GTE, this.column(), Value(value))

inline infix fun <reified T : Any, R> KProperty1<T, R>.lt(value: R) where R : Any, R : Comparable<R> =
  Comparison(LT, this.column(), Value(value))

inline infix fun <reified T : Any, R> KProperty1<T, R>.lte(value: R) where R : Any, R : Comparable<R> =
  Comparison(LTE, this.column(), Value(value))

inline infix fun <reified T : Any, reified S : Any, R> KProperty1<T, R?>.eq(property: KProperty1<S, R?>) where R : Any, R : Comparable<R> =
  Comparison(EQ, this.column(), property.column())

inline infix fun <reified T : Any, R> KProperty1<T, R>.ne(property: KProperty1<T, R>) where R : Any, R : Comparable<R> =
  Comparison(NE, this.column(), property.column())

inline infix fun <reified T : Any, R> KProperty1<T, R>.gt(property: KProperty1<T, R>) where R : Any, R : Comparable<R> =
  Comparison(GT, this.column(), property.column())

inline infix fun <reified T : Any, R> KProperty1<T, R>.gte(property: KProperty1<T, R>) where R : Any, R : Comparable<R> =
  Comparison(GTE, this.column(), property.column())

inline infix fun <reified T : Any, R> KProperty1<T, R>.lt(property: KProperty1<T, R>) where R : Any, R : Comparable<R> =
  Comparison(LT, this.column(), property.column())

inline infix fun <reified T : Any, R> KProperty1<T, R>.lte(property: KProperty1<T, R>) where R : Any, R : Comparable<R> =
  Comparison(LTE, this.column(), property.column())

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.eq(value: R) =
  Comparison(EQ, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.ne(value: R) =
  Comparison(NE, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.gt(value: R) =
  Comparison(GT, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.gte(value: R) =
  Comparison(GTE, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.lt(value: R) =
  Comparison(LT, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.lte(value: R) =
  Comparison(LTE, this.column(), Value<R?>(value))

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.eq(expression: Expression<R>) =
  Comparison(EQ, this.column(), expression)

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.ne(expression: Expression<R>) =
  Comparison(NE, this.column(), expression)

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.gt(expression: Expression<R>) =
  Comparison(GT, this.column(), expression)

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.gte(expression: Expression<R>) =
  Comparison(GTE, this.column(), expression)

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.lt(expression: Expression<R>) =
  Comparison(LT, this.column(), expression)

inline infix fun <reified T : Any, R : Comparable<R>> KProperty1<T, R>.lte(expression: Expression<R>) =
  Comparison(LTE, this.column(), expression)

infix fun <R : Comparable<R>> Expression<R>.eq(value: R): Comparison<R> =
  Comparison(EQ, this, Value(value))

infix fun <R : Comparable<R>> Expression<R>.ne(value: R): Comparison<R> =
  Comparison(NE, this, Value(value))

infix fun <R : Comparable<R>> Expression<R>.gt(value: R): Comparison<R> =
  Comparison(GT, this, Value(value))

infix fun <R : Comparable<R>> Expression<R>.gte(value: R): Comparison<R> =
  Comparison(GTE, this, Value(value))

infix fun <R : Comparable<R>> Expression<R>.lt(value: R): Comparison<R> =
  Comparison(LT, this, Value(value))

infix fun <R : Comparable<R>> Expression<R>.lte(value: R): Comparison<R> =
  Comparison(LTE, this, Value(value))

infix fun <R : Comparable<R>> Expression<R?>.eq(expression: Expression<R?>) =
  Comparison(EQ, this, expression)

infix fun <R : Comparable<R>> Expression<R?>.ne(expression: Expression<R?>) =
  Comparison(NE, this, expression)

infix fun <R : Comparable<R>> Expression<R?>.gt(expression: Expression<R?>) =
  Comparison(GT, this, expression)

infix fun <R : Comparable<R>> Expression<R?>.gte(expression: Expression<R?>) =
  Comparison(GTE, this, expression)

infix fun <R : Comparable<R>> Expression<R?>.lt(expression: Expression<R?>) =
  Comparison(LT, this, expression)

infix fun <R : Comparable<R>> Expression<R?>.lte(expression: Expression<R?>) =
  Comparison(LTE, this, expression)

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.eq(target: KProperty1<T, R?>) =
  Comparison(EQ, this, target.column())

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.ne(target: KProperty1<T, R?>) =
  Comparison(NE, this, target.column())

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.gt(target: KProperty1<T, R?>) =
  Comparison(GT, this, target.column())

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.gte(target: KProperty1<T, R?>) =
  Comparison(GTE, this, target.column())

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.lt(target: KProperty1<T, R?>) =
  Comparison(LT, this, target.column())

inline infix fun <reified T : Any, R : Comparable<R>> Expression<R?>.lte(target: KProperty1<T, R?>) =
  Comparison(LTE, this, target.column())

// Array Comparison

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>>.contains(value: List<R>) =
  ArrayComparison(CONTAINS, this.column(), value.array())

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>>.containedBy(value: List<R>) =
  ArrayComparison(CONTAINED_BY, this.column(), value.array())

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>>.overlaps(value: List<R>) =
  ArrayComparison(OVERLAPS, this.column(), value.array())

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>?>.contains(value: List<R>): Expression<Boolean> =
  ArrayComparison(CONTAINS, column(), AdaptNotNullToNull(Array(value, R::class)))

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>?>.containedBy(value: List<R>): Expression<Boolean> =
  ArrayComparison(CONTAINED_BY, column(), AdaptNotNullToNull(Array(value, R::class)))

inline infix fun <reified T : Any, reified R : Any> KProperty1<T, List<R>?>.overlaps(value: List<R>): Expression<Boolean> =
  ArrayComparison(OVERLAPS, column(), AdaptNotNullToNull(Array(value, R::class)))

inline infix fun <reified T : Any, R> KProperty1<T, List<R>>.contains(expression: Expression<List<R>>) =
  ArrayComparison(CONTAINS, this.column(), expression)

inline infix fun <reified T : Any, R> KProperty1<T, List<R>>.containedBy(expression: Expression<List<R>>) =
  ArrayComparison(CONTAINED_BY, this.column(), expression)

inline infix fun <reified T : Any, R> KProperty1<T, List<R>>.overlaps(expression: Expression<List<R>>) =
  ArrayComparison(OVERLAPS, this.column(), expression)

inline infix fun <reified T : Any, reified S : Any, R> KProperty1<T, List<R>?>.contains(property: KProperty1<S, List<R>?>) =
  ArrayComparison(CONTAINS, this.column(), property.column())

inline infix fun <reified T : Any, reified S : Any, R> KProperty1<T, List<R>?>.containedBy(property: KProperty1<S, List<R>?>) =
  ArrayComparison(CONTAINED_BY, this.column(), property.column())

inline infix fun <reified T : Any, reified S : Any, R> KProperty1<T, List<R>?>.overlaps(property: KProperty1<S, List<R>?>) =
  ArrayComparison(OVERLAPS, this.column(), property.column())

// Map Comparison
infix fun <K, V> Expression<Map<K, V>>.contains(value: Map<K, V>) =
  ArrayComparison(CONTAINS, this, Value(value))

infix fun <K, V> Expression<Map<K, V>>.contains(expression: Expression<Map<K, V>>) =
  ArrayComparison(CONTAINS, this, expression)

// Numeric Arithmetic
inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.plus(value: R) =
  NumericArithmetic(PLUS, this.column(), Value(value))

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.minus(value: R) =
  NumericArithmetic(MINUS, this.column(), Value(value))

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.times(value: R) =
  NumericArithmetic(TIMES, this.column(), Value(value))

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.div(value: R) =
  NumericArithmetic(DIV, this.column(), Value(value))

inline fun <reified T : Any, R : Number> KProperty1<T, R>.mod(value: R) =
  NumericArithmetic(MOD, this.column(), Value(value))

inline fun <reified T : Any, R : Number> KProperty1<T, R>.pow(value: R) =
  NumericArithmetic(POW, this.column(), Value(value))

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.plus(expression: Expression<R>) =
  NumericArithmetic(PLUS, this.column(), expression)

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.minus(expression: Expression<R>) =
  NumericArithmetic(MINUS, this.column(), expression)

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.times(expression: Expression<R>) =
  NumericArithmetic(TIMES, this.column(), expression)

inline operator fun <reified T : Any, R : Number> KProperty1<T, R>.div(expression: Expression<R>) =
  NumericArithmetic(DIV, this.column(), expression)

inline fun <reified T : Any, R : Number> KProperty1<T, R>.mod(expression: Expression<R>) =
  NumericArithmetic(MOD, this.column(), expression)

inline fun <reified T : Any, R : Number> KProperty1<T, R>.pow(expression: Expression<R>) =
  NumericArithmetic(POW, this.column(), expression)

operator fun <R : Number> Expression<R>.plus(value: R) =
  NumericArithmetic(PLUS, this, Value(value))

operator fun <R : Number> Expression<R>.minus(value: R) =
  NumericArithmetic(MINUS, this, Value(value))

operator fun <R : Number> Expression<R>.times(value: R) =
  NumericArithmetic(TIMES, this, Value(value))

operator fun <R : Number> Expression<R>.div(value: R) =
  NumericArithmetic(DIV, this, Value(value))

fun <R : Number> Expression<R>.mod(value: R) =
  NumericArithmetic(MOD, this, Value(value))

fun <R : Number> Expression<R>.pow(value: R) =
  NumericArithmetic(POW, this, Value(value))

operator fun <R : Number> Expression<R>.plus(expression: Expression<R>) =
  NumericArithmetic(PLUS, this, expression)

operator fun <R : Number> Expression<R>.minus(expression: Expression<R>) =
  NumericArithmetic(MINUS, this, expression)

operator fun <R : Number> Expression<R>.times(expression: Expression<R>) =
  NumericArithmetic(TIMES, this, expression)

operator fun <R : Number> Expression<R>.div(expression: Expression<R>) =
  NumericArithmetic(DIV, this, expression)

fun <R : Number> Expression<R>.mod(expression: Expression<R>) =
  NumericArithmetic(MOD, this, expression)

fun <R : Number> Expression<R>.pow(expression: Expression<R>) =
  NumericArithmetic(POW, this, expression)

// Temporal Arithmetic
inline operator fun <reified T : Any> KProperty1<T, OffsetDateTime>.plus(expression: Expression<PGInterval>) =
  DateArithmetic(PLUS_INTERVAL, this.column(), expression)

inline operator fun <reified T : Any> KProperty1<T, OffsetDateTime>.minus(expression: Expression<PGInterval>) =
  DateArithmetic(MINUS_INTERVAL, this.column(), expression)

operator fun Expression<OffsetDateTime>.plus(expression: Expression<PGInterval>) =
  DateArithmetic(PLUS_INTERVAL, this, expression)

operator fun Expression<OffsetDateTime>.minus(expression: Expression<PGInterval>) =
  DateArithmetic(MINUS_INTERVAL, this, expression)

// JsonB
inline fun <reified T : Any, R : Any> KProperty1<T, R>.jsonGet(value: R) =
  JsonBIntrospect<Any>(JsonBOperator.GET, this.column(), Value(value))

inline fun <reified T : Any, R : Any> KProperty1<T, R>.jsonGetText(value: R) =
  JsonBIntrospect<String>(JsonBOperator.GET_AS_TEXT, this.column(), Value(value))

fun <R : Any> JsonBIntrospect<R>.jsonGet(value: R) =
  JsonBIntrospect<Any>(JsonBOperator.GET, this, Value(value))

fun <R : Any> JsonBIntrospect<R>.jsonGetText(value: R) =
  JsonBIntrospect<String>(JsonBOperator.GET_AS_TEXT, this, Value(value))

// Logical

inline fun <reified T : Any, R> KProperty1<T, R>.isNull() =
  Is(this.column(), null)

inline fun <reified T : Any, R> KProperty1<T, R>.isTrue() =
  Is(this.column(), true)

inline fun <reified T : Any, R> KProperty1<T, R>.isFalse() =
  Is(this.column(), false)

inline fun <reified T : Any, R> KProperty1<T, R>.isNotNull() =
  Is(this.column(), null, not = true)

inline fun <reified T : Any, R> KProperty1<T, R>.isNotTrue() =
  Is(this.column(), true, not = true)

inline fun <reified T : Any, R> KProperty1<T, R>.isNotFalse() =
  Is(this.column(), false, not = true)

inline fun <reified T : Any, R> KProperty1<T, List<R>?>.isEmpty() =
  Empty(this.column())

inline fun <reified T : Any, R> KProperty1<T, List<R>?>.isNotEmpty() =
  Empty(this.column(), not = true)

@JvmName("isEmptyPropertyForSet")
inline fun <reified T : Any, R> KProperty1<T, Set<R>?>.isEmpty() =
  Empty(this.column())

@JvmName("isNotEmptyPropertyForSet")
inline fun <reified T : Any, R> KProperty1<T, Set<R>?>.isNotEmpty() =
  Empty(this.column(), not = true)

inline infix fun <reified T : Any, R> KProperty1<T, R>.isIn(values: List<R>) =
  In(this.column(), values.map { Value(it) })

inline infix fun <reified T : Any, R> KProperty1<T, R>.isNotIn(values: List<R>) =
  In(this.column(), values.map { Value(it) }, not = true)

inline infix fun <reified T : Any, R> KProperty1<T, R>.isIn(values: Set<R>) =
  In(this.column(), values.map { Value(it) })

inline infix fun <reified T : Any, R> KProperty1<T, R>.isNotIn(values: Set<R>) =
  In(this.column(), values.map { Value(it) }, not = true)

// Logical for column aliases

fun <R> Expression<R>.isNull() =
  Is(this, null)

fun <R> Expression<R>.isTrue() =
  Is(this, true)

fun <R> Expression<R>.isFalse() =
  Is(this, false)

fun <R> Expression<R>.isNotNull() =
  Is(this, null, not = true)

fun <R> Expression<R>.isNotTrue() =
  Is(this, true, not = true)

fun <R> Expression<R>.isNotFalse() =
  Is(this, false, not = true)

fun <R> Expression<List<R>?>.isEmpty() =
  Empty(this)

fun <R> Expression<List<R>?>.isNotEmpty() =
  Empty(this, not = true)

@JvmName("isEmptyExpressionForSet")
fun <R> Expression<Set<R>?>.isEmpty() =
  Empty(this)

@JvmName("isNotEmptyExpressionForSet")
fun <R> Expression<Set<R>?>.isNotEmpty() =
  Empty(this, not = true)

infix fun <R> Expression<R>.isIn(values: List<R>) =
  In(this, values.map { Value(it) })

infix fun <R> Expression<R>.isNotIn(values: List<R>) =
  In(this, values.map { Value(it) }, not = true)

infix fun <R> Expression<R>.isIn(values: Set<R>) =
  In(this, values.map { Value(it) })

infix fun <R> Expression<R>.isNotIn(values: Set<R>) =
  In(this, values.map { Value(it) }, not = true)

inline infix fun <reified T : Any> KProperty1<T, String?>.like(pattern: String?) =
  Like(this.column(), Value(pattern))

inline infix fun <reified T : Any> KProperty1<T, String?>.notLike(pattern: String?) =
  Like(this.column(), Value(pattern), not = true)

inline infix fun <reified T : Any> KProperty1<T, String?>.ilike(pattern: String?) =
  ILike(this.column(), Value(pattern))

inline infix fun <reified T : Any> KProperty1<T, String?>.notILike(pattern: String?) =
  ILike(this.column(), Value(pattern), not = true)

inline infix fun <reified T : Any> KProperty1<T, String?>.matches(pattern: String?) =
  Similar(this.column(), Value(pattern))

inline infix fun <reified T : Any> KProperty1<T, String?>.notMatches(pattern: String?) =
  Similar(this.column(), Value(pattern), not = true)

infix fun <R : String?> Expression<R>.like(pattern: R) =
  Like(this, Value(pattern))

infix fun <R : String?> Expression<R>.notLike(pattern: R) =
  Like(this, Value(pattern), not = true)

infix fun <R : String?> Expression<R>.ilike(pattern: R) =
  ILike(this, Value(pattern))

infix fun <R : String?> Expression<R>.notILike(pattern: R) =
  ILike(this, Value(pattern), not = true)

infix fun <R : String?> Expression<R>.matches(pattern: R) =
  Similar(this, Value(pattern))

infix fun <R : String?> Expression<R>.notMatches(pattern: R) =
  Similar(this, Value(pattern), not = true)

infix fun <R : String?> Expression<R>.like(patternExpression: Expression<R>) =
  Like(this, patternExpression)

infix fun <R : String?> Expression<R>.notLike(patternExpression: Expression<R>) =
  Like(this, patternExpression, not = true)

infix fun <R : String?> Expression<R>.ilike(patternExpression: Expression<R>) =
  ILike(this, patternExpression)

infix fun <R : String?> Expression<R>.notilike(patternExpression: Expression<R>) =
  ILike(this, patternExpression, not = true)

infix fun <R : String?> Expression<R>.matches(patternExpression: Expression<R>) =
  Similar(this, patternExpression)

infix fun <R : String?> Expression<R>.notMatches(patternExpression: Expression<R>) =
  Similar(this, patternExpression, not = true)

infix fun Expression<Boolean>.and(expression: Expression<Boolean>) =
  And(listOf(this, expression))

infix fun Expression<Boolean>.or(expression: Expression<Boolean>) =
  Or(listOf(this, expression))

fun Expression<Boolean>.inverse(): Not = Not(this)

infix fun <R> Expression<Boolean>.then(result: Expression<R>) =
  WhenThen(condition = this, result = result)

infix fun <R> Expression<Boolean>.then(result: R) =
  WhenThen(condition = this, result = Value(result))

fun <R> case(vararg whenCases: WhenThen<R>?, fallback: R) =
  Case(cases = whenCases.filterNotNull(), fallback = Value(fallback))

fun <R> case(vararg whenCases: WhenThen<R>?, fallback: Expression<R>? = null) =
  Case(cases = whenCases.filterNotNull(), fallback = fallback)

fun caseOrder(vararg whenCases: WhenThen<Int>?) =
  CaseOrder(cases = whenCases.filterNotNull())

fun exists(query: Query, not: Boolean = false, alias: String? = null): Expression<Boolean> =
  Exists(subQuery = SubQuery(query = query, alias = alias), not = not)

fun notExists(query: Query, not: Boolean = true, alias: String? = null): Expression<Boolean> =
  Exists(subQuery = SubQuery(query = query, alias = alias), not = not)

fun and(vararg expressions: Expression<Boolean>?) =
  And(expressions.filterNotNull())

fun or(vararg expressions: Expression<Boolean>?) =
  Or(expressions.filterNotNull())

fun not(expression: Expression<Boolean>) =
  Not(expression)

// Selection

// We're manually duplicating syntax between KClass<T> and TableAlias<T>.  In the future, we should consider creating
// some kind of common base object or interface so the duplication isn't necessary; maybe add a 'from' method that
// wraps a KClass as a TableReference<T> or something...
// from<Nest>().select(...).where(...)

inline fun <reified T : Any> KClass<T>.select(): Query {
  val table = Table(this)
  return Query(listOf(), listOf(), listOf(table))
}

fun <T : Any> TableAlias<T>.select(): Query {
  val table = Table(type, alias)
  return Query(listOf(), listOf(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.select(vararg properties: KProperty1<T, *>): Query {
  val table = Table(this)
  return Query(properties.map { Projection(Column(it, table), null) }, listOf(), listOf(table))
}

fun <T : Any> TableAlias<T>.select(vararg properties: KProperty1<T, *>): Query {
  val table = Table(type, alias)
  return Query(properties.map { Projection(Column(it, table), null) }, listOf(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.select(vararg expressions: Expression<*>): Query {
  val table = Table(this)
  return Query(expressions.map { Projection(it, null) }, listOf(), listOf(table))
}

fun <T : Any> TableAlias<T>.select(vararg expressions: Expression<*>): Query {
  val table = Table(type, alias)
  return Query(expressions.map { Projection(it, null) }, listOf(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.select(vararg projections: Projection): Query {
  val table = Table(this)
  return Query(projections.toList(), listOf(), listOf(table))
}

fun <T : Any> TableAlias<T>.select(vararg projections: Projection): Query {
  val table = Table(type, alias)
  return Query(projections.toList(), listOf(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.select(vararg aggregates: Aggregate): Query {
  val table = Table(this)
  return Query(listOf(), aggregates.toList(), listOf(table))
}

fun <T : Any> TableAlias<T>.select(vararg aggregates: Aggregate): Query {
  val table = Table(type, alias)
  return Query(listOf(), aggregates.toList(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.selectAll(): Query {
  val table = Table(this)
  return Query(listOf(Projection(All(table))), listOf(), listOf(table))
}

inline fun <reified T : Any> KClass<T>.selectOne(): Query {
  val table = Table(this)
  return Query(listOf(Projection(Value(1))), listOf(), listOf(table))
}

fun <T : Any> TableAlias<T>.selectAll(): Query {
  val table = Table(type, alias)
  return Query(listOf(Projection(All(table))), listOf(), listOf(table))
}

inline fun <reified T : Any, reified U : Any> KClass<T>.selectAll(type: KClass<U>): Query {
  val table = Table(this)
  val propertyNames = type.memberProperties.filter { it.findAnnotation<Transient>() == null }.map { it.name }.toSet()
  val properties = T::class.memberProperties.filter { propertyNames.contains(it.name) }
  return Query(properties.map { Projection(Column(it, table), null) }, listOf(), listOf(table))
}

fun Query.distinct(): Query = copy(distinct = true)
fun Query.random(): Query = copy(random = true)

fun all() = All<Any>(null)
inline fun <reified T : Any> KClass<T>.all() = All(Table(this))

fun projectAll() = Projection(all())
inline fun <reified T : Any> KClass<T>.projectAll() = Projection(this.all())

inline fun <reified T : Any> KClass<T>.selectCount(): Query {
  val table = Table(this)
  return Query(listOf(), listOf(countAll()), listOf(table))
}

inline fun <reified T : Any> KClass<T>.selectCountDistinct(property: KProperty1<T, *>, alias: String? = null): Query {
  val table = Table(this)
  val column = Column(property, table)
  return Query(listOf(), listOf(countDistinct(column, alias)), listOf(table))
}

inline fun <reified T : Any> Query.groupBy(vararg properties: KProperty1<T, *>): Query {
  val table = Table(T::class)
  return copy(projections = properties.map { Projection(Column(it, table), null) }, grouped = true)
}

fun Query.groupBy(vararg projections: Projection) = this.copy(projections = projections.toList(), grouped = true)

inline fun <reified T : Any> Query.distinctOn(vararg properties: KProperty1<T, *>): Query {
  val table = Table(T::class)
  return copy(distinctOnClauses = properties.map { Projection(Column(it, table), null) })
}

fun Query.where(condition: Expression<Boolean>?) = this.copy(whereClause = condition)
fun SubQuery.where(condition: Expression<Boolean>?) = this.copy(query = this.query.copy(whereClause = condition))

inline fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.asc() = ExpressionOrder(this.column(), false)
inline fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.desc() = ExpressionOrder(this.column(), true)

fun <R : Comparable<R>> Expression<R?>.asc() = ExpressionOrder(this, false)
fun <R : Comparable<R>> Expression<R?>.desc() = ExpressionOrder(this, true)

fun ExpressionOrder.nullsFirst() = this.copy(nullsPref = NullsOrderPref.NULLS_FIRST)
fun ExpressionOrder.nullsLast() = this.copy(nullsPref = NullsOrderPref.NULLS_LAST)

fun Query.orderBy(vararg order: Order) = this.copy(orderClauses = order.toList())
fun Query.orderBy(order: List<Order>) = this.copy(orderClauses = order)
fun <R : Comparable<R>> Query.orderBy(expression: Expression<R>, desc: Boolean = false) = this.copy(
  orderClauses = listOf(
    ExpressionOrder(expression, desc)
  )
)

fun Query.limit(limit: Int?) = this.copy(limitNumber = limit)
fun Query.offset(offset: Int?) = this.copy(offsetNumber = offset)
fun Query.page(page: Int?) = this.copy(offsetNumber = page?.let { it * this.limitNumber!! })

fun Query.join(join: Join) = copy(fromClauses = fromClauses + listOf(join.copy(kind = INNER)))
fun Query.leftJoin(join: Join) = copy(fromClauses = fromClauses + listOf(join.copy(kind = LEFT)))
fun Query.rightJoin(join: Join) = copy(fromClauses = fromClauses + listOf(join.copy(kind = RIGHT)))
fun Query.fullJoin(join: Join) = copy(fromClauses = fromClauses + listOf(join.copy(kind = FULL)))

fun Query.on(on: Expression<Boolean>, alias: String? = null) = Join(this, on = on, alias = alias)
fun Join.on(on: Expression<Boolean>) = copy(on = on)

inline fun <reified T : Any> Query.using(vararg using: KProperty1<T, *>) = Join(this, using = using.map { it.column() })
inline fun <reified T : Any> Join.using(vararg using: KProperty1<T, *>) = copy(using = using.map { it.column() })

inline fun <reified T : Any, reified R : Any> Query.into(into: KProperty1<T, R?>) =
  Join(this, intoField = IntoField(R::class, into.column()))

inline fun <reified T : Any, reified R : Any> Join.into(into: KProperty1<T, R?>) =
  copy(intoField = IntoField(R::class, into.column()))

fun Query.generate(dialect: Dialect) = dialect.generate(this)

fun <T : Any, R> Expression<T>.cast(newType: TypeSpec<R>): Cast<T, R> = Cast(this, newType)
inline fun <reified T : Any, R : Any, C> KProperty1<T, R?>.cast(newType: TypeSpec<C>): Cast<R, C> =
  Cast(this.column(), newType)

fun <T : Any> Expression<T>.castInt(): Expression<Int?> {
  return Cast(this, TypeSpec("INT"))
}

fun Aggregate.unnest() = copy(unnested = true)

// 'manual/pro-mode' alias
inline fun <reified T : Any> KClass<T>.alias(alias: String): TableAlias<T> = TableAlias(this, alias)

// 'guided/easy-mode' alias
// This will fail if you alias the same table multiple times.  Ideally part of the SQL-generation process should
// involve walking the AST and resolving aliases; that would allow 'symbolic aliases' where you don't actually
// choose the text but instead let Skate create them.
inline fun <reified TQuery : Any, TReturn> KClass<TQuery>.alias(block: (TableAlias<TQuery>) -> TReturn): TReturn =
  block(TableAlias(this, "autogenerated_alias_" + (this.simpleName ?: this.jvmName)))
