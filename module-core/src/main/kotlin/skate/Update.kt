package skate

// This would probably benefit from being split into UpdateWithoutSource/UpdateWithoutWhere
data class UpdateWithoutWhere<T : Any>(
  val table: Table<T>,
  val fields: List<UpdateField<T>>,
  val fromClause: UpdateFrom = EmptyUpdateFrom
)

data class Update<T : Any>(
  val table: Table<T>,
  val fields: List<UpdateField<T>>,
  val whereClause: Expression<Boolean>? = null,
  val fromClause: UpdateFrom = EmptyUpdateFrom,
  val intoFields: List<IntoField<*, *>> = emptyList(),
  val projections: List<Projection>? = null
)

data class UpdateField<T : Any>(
  val column: Column<T, *>,
  val expression: Expression<*>
)

// UPDATE blah SET blah.foo = 'bar' {UpdateFrom} WHERE ...
sealed class UpdateFrom

// Most common update statement -- we don't have a from
object EmptyUpdateFrom : UpdateFrom()

data class UpdateFromTableList(val sources: List<Table<*>>) : UpdateFrom()

// UPDATE blah SET blah.foo = 'bar' FROM Table1 t1, Table2 t2 WHERE ...
//                                       ^^^^^^^^^^^^^^^^^^^^
data class UpdateFromTableAliasList(val sources: List<TableAlias<*>>) : UpdateFrom()

// UPDATE blah SET blah.foo = 'bar' FROM (SELECT ...) alias WHERE ...
//                                        ^^^^^^^^^   ^^^^^
// In PostgreSQL, a FROM subquery requires an alias for the entire subquery; there may also be aliases to tables within
// the subquery, but you need to have an alias for the whole thing to be syntactically valid.
data class UpdateFromSubQuery<T : Any>(val alias: TableAlias<T>, val query: Query) : UpdateFrom()

// BUT: As long as you somehow anchor the subquery to the main table in the WHERE claus, you don't actually have to do
// anything with the alias.  Skate can auto-generate an ignored alias for you.
data class UpdateFromSubQueryWithIgnoredAlias(val query: Query) : UpdateFrom()
