package skate

import org.postgresql.util.PGInterval
import java.time.OffsetDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

sealed class Expression<R>
data class All<T : Any>(val table: Table<T>? = null) : Expression<Any>()
data class Column<T : Any, R>(val property: KProperty1<T, R?>, val table: Table<T>) : Expression<R>()
data class Distinct<T : Any>(val column: Column<T, *>) : Expression<Any>()
data class Attribute<T : Any, R>(val property: KProperty1<T, R>, val table: Table<T>) : Expression<R>()
data class Value<R>(val value: R) : Expression<R>()
data class Array<R : Any>(val items: List<R>, val type: KClass<R>) : Expression<List<R>>()
data class NumericArithmetic<R : Number>(
  val operator: NumericOperator,
  val left: Expression<R>,
  val right: Expression<R>
) : Expression<R>()

data class JsonBIntrospect<R : Any>(val operator: JsonBOperator, val left: Expression<*>, val right: Expression<*>) :
  Expression<R>()

data class DateArithmetic(
  val operator: DateOperator,
  val left: Expression<OffsetDateTime>,
  val right: Expression<PGInterval>
) : Expression<OffsetDateTime>()

data class Comparison<R>(val operator: ComparisonOperator, val left: Expression<R>, val right: Expression<R>) :
  Expression<Boolean>()

data class ArrayComparison<R>(
  val operator: ArrayComparisonOperator,
  val left: Expression<R>,
  val right: Expression<R>
) : Expression<Boolean>()

data class And(val expressions: List<Expression<Boolean>>) : Expression<Boolean>()
data class Or(val expressions: List<Expression<Boolean>>) : Expression<Boolean>()
data class ScaleInterval(val interval: Expression<PGInterval>, val scale: Expression<Int>) : Expression<PGInterval>()
data class Not(val expr: Expression<Boolean>) : Expression<Boolean>()
data class Empty<R>(val expr: Expression<R>, val not: Boolean = false) : Expression<Boolean>()
data class Is<R>(val expr: Expression<R>, val value: Boolean?, val not: Boolean = false) : Expression<Boolean>()
data class In<R>(val expr: Expression<R>, val values: List<Expression<R>>, val not: Boolean = false) :
  Expression<Boolean>()

data class Like<R : String?>(val expr: Expression<R>, val pattern: Expression<R>, val not: Boolean = false) :
  Expression<Boolean>()

data class ILike<R : String?>(val expr: Expression<R>, val pattern: Expression<R>, val not: Boolean = false) :
  Expression<Boolean>()

data class Similar<R : String?>(val expr: Expression<R>, val pattern: Expression<R>, val not: Boolean = false) :
  Expression<Boolean>()

data class Constructor<R>(val type: String, val value: String) : Expression<R>()
data class Cast<T : Any, R>(val expr: Expression<T>, val destinationType: TypeSpec<R>) : Expression<R>()
data class Exists(val subQuery: SubQuery, val not: Boolean = false) : Expression<Boolean>()
data class AtTimeZone<R : OffsetDateTime?>(val time: Expression<R>, val timeZone: Expression<String?>) : Expression<R>()
data class Case<R>(val cases: List<WhenThen<R>>, val fallback: Expression<R>? = null) : Expression<R>()
data class WhenThen<R>(val condition: Expression<Boolean>, val result: Expression<R>) : Expression<R>()

// Skate should probably standardize that all Expressions are of non-nullable R -- it matches more cleanly to SQL's
// concept of of types and will simplify a lot of nullability-related duplicate functions.  But until then, these
// adapters can be used to generate uniform external syntax atop the less-uniform internal model.
data class AdaptNullToNotNull<R : Any>(val nullTypeExpression: Expression<R?>) : Expression<R>()
data class AdaptNotNullToNull<R : Any>(val nonNullTypeExpression: Expression<R>) : Expression<R?>()

sealed class Function<R> : Expression<R>()
data class Function0<R>(val name: String) : Function<R>()
data class Function1<S, R>(val name: String, val arg1: Expression<S>) : Function<R>()
data class Function2<S1, S2, R>(val name: String, val arg1: Expression<S1>, val arg2: Expression<S2>) : Function<R>()
data class Function3<S1, S2, S3, R>(
  val name: String,
  val arg1: Expression<S1>,
  val arg2: Expression<S2>,
  val arg3: Expression<S3>
) : Function<R>()

data class Function4<S1, S2, S3, S4, R>(
  val name: String,
  val arg1: Expression<S1>,
  val arg2: Expression<S2>,
  val arg3: Expression<S3>,
  val arg4: Expression<S4>
) : Function<R>()

data class FunctionN<S, R>(val name: String, val args: List<Expression<S>>) : Function<R>()

enum class NumericOperator { PLUS, MINUS, TIMES, DIV, MOD, POW; }
enum class DateOperator { PLUS_INTERVAL, MINUS_INTERVAL; }
enum class ComparisonOperator { EQ, NE, GT, GTE, LT, LTE; }
enum class ArrayComparisonOperator { CONTAINS, CONTAINED_BY, OVERLAPS; }
enum class JsonBOperator { GET, GET_AS_TEXT; }

data class TypeSpec<R>(val typeName: String) : Expression<R>()

// jsonB support

// TODO: Consider un-sealing Expression, or making it an interface, so this sort of thing can be an optional add-on in a separate file
sealed class PSqlJson
sealed class PSqlJsonb
data class ConcatJsonb(val lhs: Expression<PSqlJsonb>, val rhs: Expression<PSqlJsonb>) : Expression<PSqlJsonb>()
data class BuildJsonb(val pairs: List<Pair<String, Expression<Any>>>) : Expression<PSqlJsonb>()

data class AliasLiteral(val literalValue: String) : Expression<String>()
data class TableLiteral(val kclass: KClass<*>) : Expression<String>()
