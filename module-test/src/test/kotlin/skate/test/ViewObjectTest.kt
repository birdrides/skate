package skate.test

import skate.TableName
import skate.alias
import skate.columnAs
import skate.eq
import skate.generate
import skate.generator.Postgresql
import skate.insert
import skate.into
import skate.join
import skate.leftJoin
import skate.on
import skate.select
import skate.selectAll
import skate.values
import skate.where
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.execute
import skate.executeRaw
import skate.query
import java.util.UUID

class ViewObjectTest : AbstractTest() {

  private val dialect = Postgresql()

  @BeforeEach
  fun setUp() {
    db.executeRaw(DROP_TABLE_SQL)
    db.executeRaw(CREATE_TABLE_SQL)
  }

  companion object {
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE IF NOT EXISTS incoming_transactions (
          id UUID NOT NULL PRIMARY KEY,
          gateway_transaction_id UUID,
          stuff TEXT
        );

        CREATE TABLE IF NOT EXISTS incoming_gateway_transactions (
          id UUID NOT NULL PRIMARY KEY,
          gateway_transaction_id UUID,
          more_stuff TEXT
        );

        CREATE TABLE IF NOT EXISTS provider_incoming_transactions (
          id UUID NOT NULL PRIMARY KEY,
          provider_user_id UUID NOT NULL
        );
      """

    private const val DROP_TABLE_SQL =
      """
        DROP TABLE IF EXISTS incoming_transactions;
        DROP TABLE IF EXISTS incoming_gateway_transactions;
        DROP TABLE IF EXISTS provider_incoming_transactions;
      """
  }

  @Test
  fun test() {
    val userId = UUID.randomUUID()
    val pit = PaymentProviderIncomingTransaction(providerUserId = userId)

    PaymentProviderIncomingTransaction::class
      .insert()
      .values(pit)
      .generate(dialect)
      .execute(db)

    val igt = IncomingGatewayTransaction(gatewayTransactionId = pit.id)

    IncomingGatewayTransaction::class
      .insert()
      .values(igt)
      .generate(dialect)
      .execute(db)

    val ict = IncomingTransaction(gatewayTransactionId = igt.id)

    IncomingTransaction::class
      .insert()
      .values(ict)
      .generate(dialect)
      .execute(db)

    val result = IncomingTransaction::class.alias("ict")
      .select()
      .join(
        IncomingTransaction::class
          .selectAll()
          .into(LedgerTransactionSources::incomingTransaction)
          .on(IncomingTransaction::id eq IncomingTransaction::id.columnAs("ict"))
      )
      .leftJoin(
        IncomingGatewayTransaction::class
          .selectAll()
          .into(LedgerTransactionSources::incomingGatewayTransaction)
          .on(IncomingTransaction::gatewayTransactionId eq IncomingGatewayTransaction::id)
      )
      .leftJoin(
        PaymentProviderIncomingTransaction::class
          .selectAll()
          .into(LedgerTransactionSources::paymentProviderIncomingTransaction)
          .on(IncomingGatewayTransaction::gatewayTransactionId eq PaymentProviderIncomingTransaction::id)
      )
      .where(IncomingTransaction::id.columnAs("ict") eq ict.id)
      .generate(dialect)
      .query<LedgerTransactionSources>(db)
      .first()

    assertThat(result.incomingTransaction.id).isEqualTo(ict.id)
    assertThat(result.incomingGatewayTransaction?.id).isEqualTo(igt.id)
    assertThat(result.paymentProviderIncomingTransaction?.id).isEqualTo(pit.id)
  }

  data class LedgerTransactionSources(
    val incomingTransaction: IncomingTransaction,
    val incomingGatewayTransaction: IncomingGatewayTransaction? = null,
    val paymentProviderIncomingTransaction: PaymentProviderIncomingTransaction? = null
  )

  @TableName("incoming_transactions")
  data class IncomingTransaction(
    val id: UUID = UUID.randomUUID(),
    val gatewayTransactionId: UUID? = null,
    val stuff: String? = null
  )

  @TableName("incoming_gateway_transactions")
  data class IncomingGatewayTransaction(
    val id: UUID = UUID.randomUUID(),
    val gatewayTransactionId: UUID? = null,
    val moreStuff: String? = null
  )

  @TableName("provider_incoming_transactions")
  data class PaymentProviderIncomingTransaction(
    val id: UUID = UUID.randomUUID(),
    val providerUserId: UUID,
  )
}
