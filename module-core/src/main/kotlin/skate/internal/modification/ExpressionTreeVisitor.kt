package skate.internal.modification

import skate.AliasLiteral
import skate.All
import skate.And
import skate.Array
import skate.ArrayComparison
import skate.BuildJsonb
import skate.Cast
import skate.Column
import skate.Comparison
import skate.ConcatJsonb
import skate.Constructor
import skate.Expression
import skate.Function0
import skate.Function1
import skate.Function2
import skate.Function3
import skate.FunctionN
import skate.In
import skate.Is
import skate.Like
import skate.Not
import skate.NumericArithmetic
import skate.Or
import skate.PSqlJsonb
import skate.TableLiteral
import skate.TypeSpec
import skate.Value

// Base class for a generalized visitor; experimental/primitive and may not work beyond the initial use case
@Suppress("UNCHECKED_CAST")
abstract class ExpressionTreeVisitor {
  fun <R> visit(expression: Expression<R>): Expression<R> {
    return when (expression) {
      is Column<*, R> -> visitColumn(expression)
      is Value<R> -> visitValue(expression)
      is Array<*> -> visitArray(expression) as Expression<R>
      is NumericArithmetic<*> -> visitNumericArithmetic(expression) as Expression<R>
      is Comparison<*> -> visitComparison(expression) as Expression<R>
      is ArrayComparison<*> -> visitArrayComparison(expression) as Expression<R>
      is And -> visitAnd(expression) as Expression<R>
      is Or -> visitOr(expression) as Expression<R>
      is Not -> visitNot(expression) as Expression<R>
      is Is<*> -> visitIs(expression) as Expression<R>
      is In<*> -> visitIn(expression) as Expression<R>
      is Like<*> -> visitLike(expression) as Expression<R>
      is Function0<R> -> visitFunction0(expression)
      is Function1<*, R> -> visitFunction1(expression)
      is Function2<*, *, R> -> visitFunction2(expression)
      is Function3<*, *, *, R> -> visitFunction3(expression)
      is FunctionN<*, R> -> visitFunctionN(expression)
      is Constructor<*> -> visitConstructor(expression)
      is All<*> -> visitAll(expression) as Expression<R>
      is Cast<*, R> -> visitCast(expression)
      is AliasLiteral -> visitAliasLiteral(expression)
      is TableLiteral -> visitTableLiteral(expression)
      is BuildJsonb -> visitBuildJsonb(expression) as Expression<R>
      is ConcatJsonb -> visitConcatJsonb(expression) as Expression<R>
      is TypeSpec -> visitTypeSpec(expression)
      else ->
        throw IllegalArgumentException("Unknown type for expression: $expression")
    }
  }

  open fun <T : Any, R> visitColumn(expression: Column<T, R>): Expression<R> {
    return Column(expression.property, expression.table)
  }

  open fun <R> visitValue(expression: Value<R>): Expression<R> {
    return Value(expression.value)
  }

  open fun <R : Any> visitArray(expression: Array<R>): Expression<List<R>> {
    return Array(
      expression.items.map { it },
      expression.type
    )
  }

  open fun <R : Number> visitNumericArithmetic(expression: NumericArithmetic<R>): Expression<R> {
    return NumericArithmetic(
      expression.operator,
      visit(expression.left),
      visit(expression.right)
    )
  }

  open fun <R> visitComparison(expression: Comparison<R>): Expression<Boolean> {
    return Comparison(
      expression.operator,
      visit(expression.left),
      visit(expression.right)
    )
  }

  open fun <R> visitArrayComparison(expression: ArrayComparison<R>): Expression<Boolean> {
    return ArrayComparison(
      expression.operator,
      visit(expression.left),
      visit(expression.right)
    )
  }

  open fun visitAnd(expression: And): Expression<Boolean> {
    return And(expression.expressions.map(::visit))
  }

  open fun visitOr(expression: Or): Expression<Boolean> {
    return Or(expression.expressions.map(::visit))
  }

  open fun visitNot(expression: Not): Expression<Boolean> {
    return Not(visit(expression.expr))
  }

  open fun <R> visitIs(expression: Is<R>): Expression<Boolean> {
    return Is(visit(expression.expr), expression.value, expression.not)
  }

  open fun <R> visitIn(expression: In<R>): Expression<Boolean> {
    return In(
      visit(expression.expr),
      expression.values.map(::visit)
    )
  }

  open fun <R : String?> visitLike(expression: Like<R>): Expression<Boolean> {
    return Like(visit(expression.expr), visit(expression.pattern))
  }

  open fun <R> visitFunction0(expression: Function0<R>): Expression<R> {
    return Function0(expression.name)
  }

  open fun <S, R> visitFunction1(expression: Function1<S, R>): Expression<R> {
    return Function1(
      expression.name,
      visit(expression.arg1)
    )
  }

  open fun <S1, S2, R> visitFunction2(expression: Function2<S1, S2, R>): Expression<R> {
    return Function2(
      expression.name,
      visit(expression.arg1),
      visit(expression.arg2)
    )
  }

  open fun <S1, S2, S3, R> visitFunction3(expression: Function3<S1, S2, S3, R>): Expression<R> {
    return Function3(
      expression.name,
      visit(expression.arg1),
      visit(expression.arg2),
      visit(expression.arg3)
    )
  }

  open fun <S, R> visitFunctionN(expression: FunctionN<S, R>): Expression<R> {
    return FunctionN(
      expression.name,
      expression.args.map(::visit)
    )
  }

  open fun <R> visitConstructor(expression: Constructor<*>): Expression<R> {
    return Constructor(expression.type, expression.value)
  }

  open fun <T : Any> visitAll(expression: All<T>): Expression<Any> {
    return All(expression.table)
  }

  open fun <T : Any, R> visitCast(expression: Cast<T, R>): Expression<R> {
    return Cast(visit(expression.expr), visitTypeSpec(expression.destinationType))
  }

  open fun <R> visitAliasLiteral(expression: AliasLiteral): Expression<R> {
    return AliasLiteral(expression.literalValue) as Expression<R>
  }

  open fun <R> visitTableLiteral(expression: TableLiteral): Expression<R> {
    return TableLiteral(expression.kclass) as Expression<R>
  }

  open fun visitBuildJsonb(expression: BuildJsonb): Expression<PSqlJsonb> {
    return BuildJsonb(
      expression.pairs.map {
        Pair(it.first, visit(it.second))
      }
    )
  }

  open fun visitConcatJsonb(expression: ConcatJsonb): Expression<PSqlJsonb> {
    return ConcatJsonb(
      lhs = visit(expression.lhs),
      rhs = visit(expression.rhs)
    )
  }

  open fun <T> visitTypeSpec(expression: TypeSpec<T>): TypeSpec<T> {
    return TypeSpec(expression.typeName)
  }
}
