package skate.generator

import skate.AdaptNotNullToNull
import skate.AdaptNullToNotNull
import skate.Aggregate
import skate.AggregateFunction
import skate.AliasLiteral
import skate.All
import skate.And
import skate.ArrayComparison
import skate.AtTimeZone
import skate.BuildJsonb
import skate.Case
import skate.CaseOrder
import skate.Cast
import skate.Column
import skate.Comparison
import skate.ConcatJsonb
import skate.Constructor
import skate.DateArithmetic
import skate.Delete
import skate.Distinct
import skate.Empty
import skate.EmptyUpdateFrom
import skate.Exists
import skate.Expression
import skate.ExpressionOrder
import skate.From
import skate.Function0
import skate.Function1
import skate.Function2
import skate.Function3
import skate.Function4
import skate.FunctionN
import skate.ILike
import skate.In
import skate.Insert
import skate.InsertConflictAction
import skate.InsertConflictActionWithProjections
import skate.InsertWithProjections
import skate.Is
import skate.Join
import skate.JoinKind
import skate.JsonBIntrospect
import skate.Like
import skate.Not
import skate.NullsOrderPref
import skate.NumericArithmetic
import skate.Or
import skate.Order
import skate.Projection
import skate.Query
import skate.ScaleInterval
import skate.Similar
import skate.SubQuery
import skate.Table
import skate.TableLiteral
import skate.Update
import skate.UpdateFrom
import skate.UpdateFromSubQuery
import skate.UpdateFromSubQueryWithIgnoredAlias
import skate.UpdateFromTableAliasList
import skate.UpdateFromTableList
import skate.Value
import skate.WhenThen
import skate.Array
import java.time.OffsetDateTime
import java.util.TimeZone

/**
 * A generator for [Postgresql] dialect
 */
class Postgresql : Dialect {

  private val insertGenerator = PostgresqlInsertGenerator(this)

  override fun <T : Any> generate(insert: Insert<T>): InsertStatement<T> {
    return insertGenerator.generateInsert(insert.insert, insert.source, null, null)
  }

  override fun <T : Any> generate(insert: InsertWithProjections<T>): InsertStatement<T> {
    return insertGenerator.generateInsert(insert.insert.insert, insert.insert.source, insert.projections, null)
  }

  override fun <T : Any> generate(insertConflictAction: InsertConflictAction<T>): InsertStatement<T> {
    val insert = insertConflictAction.conflict.insert
    return insertGenerator.generateInsert(insert.insert, insert.source, null, insertConflictAction)
  }

  override fun <T : Any> generate(insertConflictActionWithProjections: InsertConflictActionWithProjections<T>): InsertStatement<T> {
    val insertConflictAction = insertConflictActionWithProjections.insertConflictAction
    val insert = insertConflictAction.conflict.insert
    val projections = insertConflictActionWithProjections.projections
    return insertGenerator.generateInsert(insert.insert, insert.source, projections, insertConflictAction)
  }

  override fun generate(query: Query): SelectStatement {
    val projectionFragments = query.projections.map { generate(it) }
    val aggregateFragments = query.aggregates.map { generate(it) }
    val fromFragments = query.fromClauses.map { generate(it) }
    val fromProjectionFragments = query.fromClauses.flatMap { from ->
      if (from is Join) {
        val (start, end) = if (from.intoField != null) {
          Pair(
            listOf(Fragment("NULL AS \"${from.intoField.joinStart()}\"")),
            listOf(Fragment("NULL AS \"${from.intoField.joinEnd()}\""))
          )
        } else {
          Pair(listOf(), listOf())
        }
        start + from.query.projections.map { generate(it) } + end
      } else {
        listOf()
      }
    }
    val whereFragment = query.whereClause?.let { generate(it) }
    val orderFragments = query.orderClauses?.map { generate(it) }
    val distinctOnFragments = query.distinctOnClauses?.map { generate(it) }

    return SelectStatement(
      "SELECT " + if (query.distinct) {
        "DISTINCT "
      } else {
        ""
      } +
        if (!query.distinct && query.distinctOnClauses != null && distinctOnFragments != null) {
          "DISTINCT ON (" + distinctOnFragments.joinToString(", ") { it.sql } + ") "
        } else {
          ""
        } +
        (aggregateFragments + projectionFragments + fromProjectionFragments).joinToString(", ") { it.sql } +
        " FROM " + fromFragments.joinToString(" ") { it.sql } +
        if (whereFragment != null) {
          " WHERE " + whereFragment.sql
        } else {
          ""
        } +
        if (query.grouped) {
          " GROUP BY " + query.projections.joinToString(", ") { generateAlias(it) }
        } else {
          ""
        } +
        if (query.random) {
          " ORDER BY RANDOM()"
        } else {
          ""
        } +
        if (orderFragments != null && !query.random) {
          " ORDER BY " + orderFragments.joinToString(", ") { it.sql }
        } else {
          ""
        } +
        (if (query.limitNumber != null) " LIMIT ${query.limitNumber}" else "") +
        (if (query.offsetNumber != null) " OFFSET ${query.offsetNumber}" else ""),
      aggregateFragments.flatMap { it.values } +
        projectionFragments.flatMap { it.values } +
        fromFragments.flatMap { it.values } +
        fromProjectionFragments.flatMap { it.values } +
        (whereFragment?.values ?: listOf()) +
        (orderFragments?.flatMap { it.values } ?: listOf()),
      query
    )
  }

  override fun <T : Any> generate(update: Update<T>): UpdateStatement {
    val table = generateTableAsNoun(update.table).sql
    val values = ArrayList<Any>(update.fields.size)

    if (update.fields.isEmpty()) {
      throw IllegalArgumentException("cannot update without fields")
    }

    val updates = update.fields.map { field ->
      val fieldFragment = generate(field.expression)
      values.addAll(fieldFragment.values)
      "${field.column.sql()} = ${fieldFragment.sql}"
    }
    val whereFragment = update.whereClause?.let { generate(it) }
    val fromFragment = generate(update.fromClause)
    val projectionFragments = update.projections?.map { generate(it) }
    val fromProjectionFragments = update.intoFields.flatMap { intoField ->
      listOf(
        Fragment("NULL AS \"${intoField.joinStart()}\""),
        generate(Projection(All(Table(intoField.type)))),
        Fragment("NULL AS \"${intoField.joinEnd()}\"")
      )
    }

    return UpdateStatement(
      "UPDATE $table" +
        " SET ${updates.joinToString(", ")}" +
        if (fromFragment != null) {
          " FROM " + fromFragment.sql
        } else {
          ""
        } +
        if (whereFragment != null) {
          " WHERE ${whereFragment.sql}"
        } else {
          ""
        } +
        if (projectionFragments != null) " RETURNING " + (projectionFragments + fromProjectionFragments).joinToString(", ") { it.sql } else "",
      values +
        (whereFragment?.values ?: listOf()) +
        (fromFragment?.values ?: listOf()) +
        (projectionFragments?.flatMap { it.values } ?: listOf()),
      update = update
    )
  }

  override fun <T : Any> generate(delete: Delete<T>): DeleteStatement {
    val table = generateTableAsNoun(delete.table).sql
    val whereFragment = delete.whereClause?.let { generate(it) }
    val whereSql = if (whereFragment != null) {
      "WHERE ${whereFragment.sql}"
    } else {
      ""
    }
    return DeleteStatement("DELETE FROM $table $whereSql", whereFragment?.values ?: listOf())
  }

  internal fun <R> generate(expression: Expression<R>): Fragment {
    return when (expression) {
      is Column<*, *> -> generate(expression)
      is Value<*> -> generate(expression)
      is Array<*> -> generate(expression)
      is NumericArithmetic<*> -> generate(expression)
      is DateArithmetic -> generate(expression)
      is Comparison<*> -> generate(expression)
      is ArrayComparison<*> -> generate(expression)
      is And -> generate(expression)
      is Or -> generate(expression)
      is Not -> generate(expression)
      is Is<*> -> generate(expression)
      is Empty<*> -> generate(expression)
      is In<*> -> generate(expression)
      is Like<*> -> generate(expression)
      is ILike<*> -> generate(expression)
      is Similar<*> -> generate(expression)
      is Function0<*> -> generate(expression)
      is Function1<*, *> -> generate(expression)
      is Function2<*, *, *> -> generate(expression)
      is Function3<*, *, *, *> -> generate(expression)
      is Function4<*, *, *, *, *> -> generate(expression)
      is FunctionN<*, *> -> generate(expression)
      is Constructor<*> -> generate(expression)
      is All<*> -> generate(expression)
      is Cast<*, *> -> generate(expression)
      is AliasLiteral -> generateAliasLiteral(expression)
      is TableLiteral -> generateTableLiteral(expression)
      is BuildJsonb -> generateBuildJsonb(expression)
      is ConcatJsonb -> generateConcatJsonb(expression)
      is AdaptNotNullToNull<*> -> generate(expression.nonNullTypeExpression)
      is AdaptNullToNotNull<*> -> generate(expression.nullTypeExpression)
      is ScaleInterval -> generateScaleInterval(expression)
      is Exists -> generate(expression)
      is JsonBIntrospect<*> -> generate(expression)
      is AtTimeZone<*> -> generate(expression)
      is Distinct<*> -> generate(expression)
      is Case<*> -> generate(expression)
      else -> throw IllegalArgumentException("unable to generate sql for $expression")
    }
  }

  private fun <R> generateInfix(operator: String, expressions: List<Expression<in R>>): Fragment {
    val expressionFragments = expressions.map { generate(it) }
    return Fragment(
      "(${expressionFragments.joinToString(" $operator ") { it.sql }})",
      expressionFragments.flatMap { it.values }
    )
  }

  private fun <R : Number> generate(numeric: NumericArithmetic<R>): Fragment {
    return generateInfix(numeric.operator.sql(), listOf(numeric.left, numeric.right))
  }

  private fun generate(dateArithmetic: DateArithmetic): Fragment {
    // could not use generateInfix here bacause DateArithmetic.left and DateArithmetic.right don't play together well
    val fragments = listOf(
      generate(dateArithmetic.left),
      generate(dateArithmetic.right)
    )

    return Fragment(
      "(${fragments.joinToString(" ${dateArithmetic.operator.sql()} ") { it.sql }})",
      fragments.flatMap { it.values }
    )
  }

  private fun <R> generate(comparison: Comparison<R>): Fragment {
    return generateInfix(comparison.operator.sql(), listOf(comparison.left, comparison.right))
  }

  private fun <R> generate(comparison: ArrayComparison<R>): Fragment {
    return generateInfix(comparison.operator.sql(), listOf(comparison.left, comparison.right))
  }

  private fun generate(and: And): Fragment {
    return generateInfix("AND", and.expressions)
  }

  private fun generate(or: Or): Fragment {
    return generateInfix("OR", or.expressions)
  }

  private fun generate(not: Not): Fragment {
    val expressionFragment = generate(not.expr)
    return Fragment(
      "(NOT ${expressionFragment.sql})",
      expressionFragment.values
    )
  }

  private fun <R> generate(emptyExpression: Empty<R>): Fragment {
    val expressionFragment = generate(emptyExpression.expr)
    val operator = if (emptyExpression.not) "!=" else "="
    return Fragment(
      "(${expressionFragment.sql} $operator ${"'{}'"})",
      expressionFragment.values
    )
  }

  private fun <R> generate(isExpression: Is<R>): Fragment {
    val expressionFragment = generate(isExpression.expr)
    val operator = if (isExpression.not) "IS NOT" else "IS"
    return Fragment(
      "(${expressionFragment.sql} $operator ${isExpression.value.toString().toUpperCase()})",
      expressionFragment.values
    )
  }

  private fun <R> generate(inExpression: In<R>): Fragment {
    val expressionFragment = generate(inExpression.expr)

    if (inExpression.values.isEmpty()) {
      // We don't want to generate "x IN ()" or "x NOT IN ()" because that's illegal SQL.
      // Cheating to "x IN (NULL)" mostly preserves expected semantics, but "x NOT IN (NULL)" won't work.
      // If we just convert "in (empty)" to "false" and "not in (empty)" to "true", we remove references to columns in
      // the root inExpression, and may therefore remove references to tables that were referenced in a join list, which
      // could also break a query.

      // Solution:
      //   x IN ()      -> ((x)=NULL) IS NOT NULL
      //   x NOT IN ()  -> ((x)=NULL) IS NULL

      val comparison = if (inExpression.not) "IS" else "IS NOT"

      return Fragment("(((${expressionFragment.sql}) = NULL) $comparison NULL)", expressionFragment.values)
    } else {
      val valueFragments = inExpression.values.map { generate(it) }
      val operator = if (inExpression.not) "NOT IN" else "IN"
      return Fragment(
        "(${expressionFragment.sql} $operator (${valueFragments.joinToString(", ") { it.sql }}))",
        expressionFragment.values + valueFragments.flatMap { it.values }
      )
    }
  }

  private fun <R : String?> generate(inExpression: Like<R>): Fragment {
    val expressionFragment = generate(inExpression.expr)
    val patternFragment = generate(inExpression.pattern)
    val operator = if (inExpression.not) "NOT LIKE" else "LIKE"
    return Fragment(
      "(${expressionFragment.sql} $operator ${patternFragment.sql})",
      expressionFragment.values + patternFragment.values
    )
  }

  private fun <R : String?> generate(inExpression: ILike<R>): Fragment {
    val expressionFragment = generate(inExpression.expr)
    val patternFragment = generate(inExpression.pattern)
    val operator = if (inExpression.not) "NOT ILIKE" else "ILIKE"
    return Fragment(
      "(${expressionFragment.sql} $operator ${patternFragment.sql})",
      expressionFragment.values + patternFragment.values
    )
  }

  private fun <R : String?> generate(inExpression: Similar<R>): Fragment {
    val expressionFragment = generate(inExpression.expr)
    val patternFragment = generate(inExpression.pattern)
    val operator = if (inExpression.not) "!~" else "~"
    return Fragment(
      "(${expressionFragment.sql} $operator ${patternFragment.sql})",
      expressionFragment.values + patternFragment.values
    )
  }

  private fun <R> generate(function: Function0<R>): Fragment {
    return Fragment("${function.name}()", listOf())
  }

  private fun <S, R> generate(function: Function1<S, R>): Fragment {
    val argFragment = generate(function.arg1)
    return Fragment(
      "${function.name}(${argFragment.sql})",
      argFragment.values
    )
  }

  private fun <S1, S2, R> generate(function: Function2<S1, S2, R>): Fragment {
    val argFragments = listOf(function.arg1, function.arg2).map { generate(it) }
    return Fragment(
      "${function.name}(${argFragments.joinToString(", ") { it.sql }})",
      argFragments.flatMap { it.values }
    )
  }

  private fun <S1, S2, S3, R> generate(function: Function3<S1, S2, S3, R>): Fragment {
    val argFragments = listOf(function.arg1, function.arg2, function.arg3).map { generate(it) }
    return Fragment(
      "${function.name}(${argFragments.joinToString(", ") { it.sql }})",
      argFragments.flatMap { it.values }
    )
  }

  private fun <S1, S2, S3, S4, R> generate(function: Function4<S1, S2, S3, S4, R>): Fragment {
    val argFragments = listOf(function.arg1, function.arg2, function.arg3, function.arg4).map { generate(it) }
    return Fragment(
      "${function.name}(${argFragments.joinToString(", ") { it.sql }})",
      argFragments.flatMap { it.values }
    )
  }

  private fun <S, R> generate(function: FunctionN<S, R>): Fragment {
    val argFragments = function.args.map { generate(it) }
    return Fragment(
      "${function.name}(${argFragments.joinToString(", ") { it.sql }})",
      argFragments.flatMap { it.values }
    )
  }

  private fun <R> generate(constructor: Constructor<R>): Fragment {
    val value = constructor.value.replace("'", "''")
    return Fragment("${constructor.type} '$value'")
  }

  private fun <R> generate(value: Value<R>): Fragment {
    return if (value.value != null) {
      Fragment("?", listOf(value.value as Any))
    } else {
      Fragment("NULL", listOf())
    }
  }

  private fun <R : Any> generate(value: Array<R>): Fragment {
    return Fragment("?", listOf(value))
  }

  private fun <T : Any, R> generate(column: Column<T, R>): Fragment {
    val table = generateTableAsAdjective(column.table).sql
    val field = column.sql()
    return Fragment("$table.$field")
  }

  private fun <T : Any> generateTableAsAdjective(table: Table<T>): Fragment {
    return Fragment((table.alias ?: table.sql()).let { "\"$it\"" })
  }

  internal fun <T : Any> generateTableAsNoun(table: Table<T>): Fragment {
    val name = table.sql()
    return Fragment("\"$name\"" + (if (table.alias != null) " \"${table.alias}\"" else ""))
  }

  private fun <T : Any> generate(all: All<T>): Fragment {
    return if (all.table != null) {
      val table = generateTableAsAdjective(all.table).sql
      Fragment("$table.*", listOf())
    } else {
      Fragment("*", listOf())
    }
  }

  private fun <T : Any, R> generate(cast: Cast<T, R>): Fragment {
    val underlyingFragment = generate(cast.expr)
    return Fragment(
      "(${underlyingFragment.sql})::${cast.destinationType.typeName}",
      underlyingFragment.values
    )
  }

  internal fun generate(projection: Projection): Fragment {
    val expressionFragment = generate(projection.expression)
    return Fragment(
      listOfNotNull(expressionFragment.sql, projection.alias).joinToString(" AS "),
      expressionFragment.values
    )
  }

  // Prefer alias, otherwise fallback to sql
  private fun generateAlias(projection: Projection): String {
    return projection.alias ?: generate(projection.expression).sql
  }

  private fun generate(aggregate: Aggregate): Fragment {
    val expressionFragment = generate(aggregate.expression)
    return Fragment(
      listOfNotNull(
        expressionFragment.sql.let {
          if (aggregate.unnested) "unnest($it)" else it
        },
        aggregate.alias
      ).joinToString(" AS "),
      expressionFragment.values
    )
  }

  private fun <S, R> generate(aggregate: AggregateFunction<S, R>): Fragment {
    val expressionFragment = generate(aggregate.expression)
    val argumentFragments = aggregate.arguments.map { generate(it) }
    val params = listOf(expressionFragment) + argumentFragments
    return Fragment(
      "${aggregate.name}(${params.joinToString(", ") { it.sql }})",
      expressionFragment.values + argumentFragments.flatMap { it.values }
    )
  }

  private fun generate(order: Order): Fragment {
    return when (order) {
      is ExpressionOrder -> generate(order)
      is CaseOrder -> generate(order)
    }
  }

  internal fun generate(order: ExpressionOrder): Fragment {
    val expressionQuery = generate(order.expression)
    return Fragment(
      listOfNotNull(
        expressionQuery.sql,
        if (order.descending) "DESC" else "ASC",
        when (order.nullsPref) {
          NullsOrderPref.NULLS_FIRST -> "NULLS FIRST"
          NullsOrderPref.NULLS_LAST -> "NULLS LAST"
          else -> null
        }
      )
        .joinToString(" "),
      expressionQuery.values
    )
  }

  internal fun generate(caseOrder: CaseOrder): Fragment {
    if (caseOrder.cases.isEmpty()) {
      throw IllegalArgumentException("There must be at least one WHEN THEN statement in an ORDER BY CASE block")
    }
    val cases = generate(caseOrder.cases)
    return Fragment(
      listOfNotNull("CASE", cases.sql, "END").joinToString(" "),
      cases.values
    )
  }

  private fun <R> generate(cases: List<WhenThen<R>>): Fragment {
    val caseQueries = cases.map { generate(it) }
    return Fragment(
      caseQueries.joinToString(" ") { it.sql },
      caseQueries.map { it.values }.flatten()
    )
  }

  private fun <R> generate(case: WhenThen<R>): Fragment {
    val conditionQuery = generate(case.condition)
    val resultQuery = generate(case.result)
    return Fragment(
      listOfNotNull("WHEN", conditionQuery.sql, "THEN", resultQuery.sql).joinToString(" "),
      conditionQuery.values + resultQuery.values
    )
  }

  private fun <R> generate(case: Case<R>): Fragment {
    if (case.cases.isEmpty()) {
      throw IllegalArgumentException("There must be at least one WHEN THEN statement in a CASE block")
    }
    val cases = generate(case.cases)
    val fallback = case.fallback?.let { generate(it) }

    return Fragment(
      listOf("CASE", cases.sql, *fallback?.let { arrayOf("ELSE", it.sql) }.orEmpty(), "END").joinToString(" "),
      cases.values + fallback?.values.orEmpty()
    )
  }

  private fun generate(from: From): Fragment {
    return when (from) {
      is Table<*> -> generateTableAsNoun(from)
      is Join -> generate(from)
      is SubQuery -> generate(from)
    }
  }

  private fun generate(subQuery: SubQuery): Fragment {
    val queryGenerated = generate(subQuery.query)
    return Fragment(
      "(${queryGenerated.sql})" +
        if (subQuery.alias != null) " ${subQuery.alias}" else "",
      values = queryGenerated.values
    )
  }

  private fun generate(join: Join): Fragment {
    val joinKind = join.kind ?: JoinKind.INNER

    val fromClause = join.query.fromClauses.firstOrNull()
    val (targetSql, targetValues) = when {
      join.alias != null -> {
        val fragment = generate(join.query)
        Pair(
          "(${fragment.sql}) \"${join.alias}\"",
          fragment.values
        )
      }

      join.query.fromClauses.size == 1 && fromClause is Table<*> -> {
        Pair(
          generateTableAsNoun(fromClause).sql,
          emptyList()
        )
      }

      else -> throw IllegalArgumentException("join must specify alias or exactly one table")
    }

    return when {
      join.on != null -> {
        val onFragment = generate(join.on)
        Fragment(
          "${joinKind.sql()} $targetSql ON ${onFragment.sql}",
          targetValues + onFragment.values
        )
      }

      join.using != null -> Fragment(
        "${joinKind.sql()} $targetSql USING ${join.using.joinToString(", ") { it.sql() }}",
        listOf()
      )

      else -> // TODO support aa-style joins
        throw IllegalArgumentException("using or on required for join")
    }
  }

  private fun generate(from: UpdateFrom): Fragment? {
    return when (from) {
      is EmptyUpdateFrom -> null
      is UpdateFromTableList -> generate(from)
      is UpdateFromTableAliasList -> generate(from)
      is UpdateFromSubQuery<*> -> generate(from)
      is UpdateFromSubQueryWithIgnoredAlias -> generate(from)
    }
  }

  private fun generate(tableList: UpdateFromTableList): Fragment {
    return Fragment(tableList.sources.joinToString(", ") { generateTableAsNoun(Table(it.type)).sql })
  }

  private fun generate(tableList: UpdateFromTableAliasList): Fragment {
    return Fragment(tableList.sources.joinToString(", ") { generateTableAsNoun(Table(it.type, it.alias)).sql })
  }

  private fun <T : Any> generate(subQuery: UpdateFromSubQuery<T>): Fragment {
    val selectStatement = generate(subQuery.query)
    return Fragment(
      "(${selectStatement.sql})" + " \"${subQuery.alias.alias}\"",
      selectStatement.values
    )
  }

  private fun generate(subQuery: UpdateFromSubQueryWithIgnoredAlias): Fragment {
    // TODO: this should interact with resolveAliasConflicts in QueryExtensions.kt
    val alias = "autogenerated_alias"
    val selectStatement = generate(subQuery.query)
    return Fragment(
      "(${selectStatement.sql})" + " \"$alias\"",
      selectStatement.values
    )
  }

  private fun generateAliasLiteral(literal: AliasLiteral): Fragment {
    return Fragment(sql = "\"" + literal.literalValue + "\"")
  }

  private fun generateTableLiteral(literal: TableLiteral): Fragment {
    val name = Table(literal.kclass).sql()
    return Fragment(sql = "\"" + name + "\"")
  }

  private fun generateBuildJsonb(build: BuildJsonb): Fragment {
    if (build.pairs.isEmpty()) {
      return Fragment(
        "jsonb_build_object()",
        emptyList()
      )
    }

    val paramsList = build.pairs.map { pair ->
      val rhsFrag = generate(pair.second)

      Fragment("'" + pair.first + "', " + rhsFrag.sql, rhsFrag.values)
    }.reduce { acc, fragment ->
      Fragment(acc.sql + ", " + fragment.sql, acc.values + fragment.values)
    }

    return Fragment(
      "jsonb_build_object(" + paramsList.sql + ")",
      paramsList.values
    )
  }

  private fun generateConcatJsonb(concat: ConcatJsonb): Fragment {
    val lhsFrag = generate(concat.lhs)
    val rhsFrag = generate(concat.rhs)

    return Fragment(
      lhsFrag.sql + " || " + rhsFrag.sql,
      lhsFrag.values + rhsFrag.values
    )
  }

  private fun generateScaleInterval(node: ScaleInterval): Fragment {
    val lhsFrag = generate(node.interval)
    val rhsFrag = generate(node.scale)

    return Fragment(
      "(" + lhsFrag.sql + " * " + rhsFrag.sql + ")",
      lhsFrag.values + rhsFrag.values
    )
  }

  fun generate(exists: Exists): Fragment {
    val subQueryGenerated = generate(exists.subQuery)
    return Fragment(
      if (exists.not) {
        "NOT "
      } else {
        ""
      } + "EXISTS " + subQueryGenerated.sql,
      subQueryGenerated.values
    )
  }

  fun <R : Any> generate(jsonIntrospect: JsonBIntrospect<R>): Fragment {
    val left = generate(jsonIntrospect.left)
    val right = generate(jsonIntrospect.right)

    return Fragment(
      left.sql + jsonIntrospect.operator.sql() + right.sql,
      left.values + right.values
    )
  }

  fun <R : OffsetDateTime?> generate(atTimeZone: AtTimeZone<R>): Fragment {
    // Only real time zones or null strings are valid time zone input
    if (atTimeZone.timeZone is Value) {
      val timeZoneString = atTimeZone.timeZone.value
      if (timeZoneString != null && timeZoneString !in AVAILABLE_TIME_ZONE_IDS) {
        throw IllegalArgumentException("'$timeZoneString' is not a valid time zone")
      }
    }

    val timeZone = generate(atTimeZone.timeZone)
    val time = generate(atTimeZone.time)

    return Fragment(
      "(${time.sql} AT TIME ZONE ${timeZone.sql})",
      time.values + timeZone.values
    )
  }

  private fun generate(distinct: Distinct<*>): Fragment {
    return Fragment(
      "DISTINCT ${distinct.column.sql()}",
      emptyList()
    )
  }

  companion object {
    private val AVAILABLE_TIME_ZONE_IDS = TimeZone.getAvailableIDs()
  }
}
