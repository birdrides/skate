package skate

import org.postgis.Geometry
import org.postgresql.util.PGInterval
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.reflect.KProperty1

// Functions

fun now() = Function0<OffsetDateTime>("now")

fun <R> coalesce(vararg args: Expression<R?>) = FunctionN<R?, R?>("coalesce", args.toList())
inline fun <reified T : Any, R> coalesce(vararg args: KProperty1<T, R?>) =
  FunctionN<R?, R?>("coalesce", args.map { it.column() })

fun <R : Any?> least(vararg args: Expression<R>) = FunctionN<R, R>("least", args.toList())
fun <R : Any?> greatest(vararg args: Expression<R>) = FunctionN<R, R>("greatest", args.toList())

inline fun <reified T : Any, R : Number> least(vararg args: KProperty1<T, R>) =
  FunctionN<R, R>("least", args.map { it.column() })

inline fun <reified T : Any, R : Number> greatest(vararg args: KProperty1<T, R>) =
  FunctionN<R, R>("greatest", args.map { it.column() })

inline fun <reified T : Any, R : String?> KProperty1<T, R>.lower() = Function1<R, R>("lower", this.column())
inline fun <reified T : Any, R : String?> KProperty1<T, R>.upper() = Function1<R, R>("upper", this.column())

inline fun <reified T : Any, R : String?> KProperty1<T, R>.charLength() =
  Function1<R, Int>("char_length", this.column())

inline fun <reified T : Any, R : UUID?> KProperty1<T, R>.text() = Function1<R, R>("text", this.column())

inline fun <reified T : Any, R : OffsetDateTime?> KProperty1<T, R>.dateTrunc(interval: Expression<PGInterval>) =
  Function2<PGInterval, R, R>("date_trunc", interval, this.column())

inline fun <reified T : Any, R : OffsetDateTime?> KProperty1<T, R>.dateTrunc(precision: String) =
  Function2<String, R, R>("date_trunc", Value(precision), this.column())

fun <R : OffsetDateTime?> Expression<R>.dateTrunc(interval: Expression<PGInterval>) =
  Function2<PGInterval, R, R>("date_trunc", interval, this)

fun <R : OffsetDateTime?> Expression<R>.dateTrunc(precision: String) =
  Function2<String, R, R>("date_trunc", Value(precision), this)

fun interval(value: String) = Constructor<PGInterval>("interval", value)

operator fun Expression<PGInterval>.times(expression: Expression<Int>): Expression<PGInterval> {
  return ScaleInterval(this, expression)
}

// Postgis
inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.distanceFrom(geometry: R) =
  Function2<R, R, Float?>("st_distance", this.column(), Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.distanceFrom(geometry: Expression<R>) =
  Function2<R, R, Float?>("st_distance", this.column(), geometry)

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.withinDistance(geometry: R, distance: Number) =
  Function3<R, R, Number, Boolean>("st_dwithin", this.column(), Value(geometry), Value(distance))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.withinDistance(
  geometry: Expression<R>,
  distance: Expression<Number>
) = Function3<R, R, Number, Boolean>("st_dwithin", this.column(), geometry, distance)

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.within(geometry: R) =
  Function2<R, R, Boolean>("st_within", this.column(), Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.within(geometry: Expression<R>) =
  Function2<R, R, Boolean>("st_within", this.column(), geometry)

fun <R : Geometry?> Expression<R>.within(geometry: R) = Function2<R, R, Boolean>("st_within", this, Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.overlaps(geometry: R) =
  Function2<R, R, Boolean>("st_overlaps", this.column(), Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.overlaps(geometry: Expression<R>) =
  Function2<R, R, Boolean>("st_overlaps", this.column(), geometry)

fun <R : Geometry?> Expression<R>.overlaps(geometry: R) = Function2<R, R, Boolean>("st_overlaps", this, Value(geometry))

inline fun <reified T : Any, R : OffsetDateTime?> KProperty1<T, R>.atTimeZone(timeZone: String?) =
  AtTimeZone(this.column(), Value(timeZone))

fun <R : OffsetDateTime?> Expression<R>.atTimeZone(timeZone: String?) = AtTimeZone(this, Value(timeZone))
inline fun <reified T : Any, R : OffsetDateTime?> KProperty1<T, R>.atTimeZone(timeZone: Expression<String?>) =
  AtTimeZone(this.column(), timeZone)

fun <R : OffsetDateTime?> Expression<R>.atTimeZone(timeZone: Expression<String?>) = AtTimeZone(this, timeZone)

inline fun <reified T : Any, R1 : Geometry?, R2> Expression<R1>.withinDistance(
  geometry: R1,
  radius: KProperty1<T, R2>,
  useSpheroid: Boolean
) = Function4<R1, R1, R2, Boolean, Boolean>("st_dwithin", this, Value(geometry), radius.column(), Value(useSpheroid))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.intersects(geometry: R) =
  Function2<R, R, Boolean>("st_intersects", this.column(), Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.intersects(geometry: Expression<R>) =
  Function2<R, R, Boolean>("st_intersects", this.column(), geometry)

fun <R : Geometry?> Expression<R>.intersects(geometry: R) =
  Function2<R, R, Boolean>("st_intersects", this, Value(geometry))

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.contains(geometry: R) =
  Function2<R, R, Boolean>("st_contains", this.column(), Value(geometry))

fun <R : Geometry?> Expression<R>.contains(geometry: R) = Function2<R, R, Boolean>("st_contains", this, Value(geometry))

inline fun <reified T : Any, R1 : Geometry?, R2> KProperty1<T, R1>.buffer(radius: KProperty1<T, R2>) =
  Function2<R1, R2, Geometry>("st_buffer", this.column(), radius.column())

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.area() = Function1<R, Float?>("st_area", this.column())
fun <R : Geometry?> Expression<R>.area() = Function1<R, Float?>("st_area", this)

inline fun <reified T : Any, R : Geometry?> KProperty1<T, R>.clusterWithin(
  radius: Double,
  alias: String? = null
): Aggregate {
  return Aggregate(AggregateFunction<R, Int>("st_clusterwithin", this.column(), listOf(Value(radius))), alias)
}

fun <R : Geometry?> Expression<R>.clusterWithin(radius: Double, alias: String? = null): Aggregate {
  return Aggregate(AggregateFunction<R, Int>("st_clusterwithin", this, listOf(Value(radius))), alias)
}

// Aggregates
fun <S, R> aggregate(name: String, expression: Expression<S>, alias: String?): Aggregate {
  return Aggregate(AggregateFunction<S, R>(name, expression), alias)
}

fun countAll(alias: String? = null) = Aggregate(AggregateFunction<Any, Int>("count", all()), alias)
fun <T : Any> countDistinct(columns: Column<T, *>, alias: String? = null) =
  Aggregate(AggregateFunction<Any, Int>("count", Distinct(columns)), alias)

inline fun <reified T : Any, R> KProperty1<T, R?>.count(alias: String? = null) =
  aggregate<R?, Int>("count", this.column(), alias)

inline fun <reified T : Any, R> KProperty1<T, R?>.first(alias: String? = null) =
  aggregate<R?, R?>("first", this.column(), alias)

inline fun <reified T : Any, R : Number> KProperty1<T, R?>.avg(alias: String? = null) =
  aggregate<R?, R?>("avg", this.column(), alias)

inline fun <reified T : Any, R : Number> KProperty1<T, R?>.sum(alias: String? = null) =
  aggregate<R?, R?>("sum", this.column(), alias)

inline fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.min(alias: String? = null) =
  aggregate<R?, R?>("min", this.column(), alias)

inline fun <reified T : Any, R : Comparable<R>> KProperty1<T, R?>.max(alias: String? = null) =
  aggregate<R?, R?>("max", this.column(), alias)

inline fun <reified R> Case<R>.sum(alias: String? = null) = aggregate<R, R>("sum", this, alias)
