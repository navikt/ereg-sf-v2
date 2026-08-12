package no.nav.sf.keytool.db

import ENHETSREGISTER_SNAPSHOT
import ENHET_SNAPSHOT
import EnhetSnapshotTable
import EnhetsregisterSnapshotTable
import UNDERENHET_SNAPSHOT
import UnderenhetSnapshotTable
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import no.nav.ereg.env
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant

const val NAIS_DB_JDBC_URL = "NAIS_DATABASE_EREG_SF_V2_EREG_JDBC_URL"

object PostgresDatabase {
    private val log = KotlinLogging.logger { }

    private val dbJdbcUrl = env(NAIS_DB_JDBC_URL)

    // Note: exposed Database connect prepares for connections but does not actually open connections
    // That is handled via transaction {} ensuring connections are opened and closed properly
    val database = Database.connect(HikariDataSource(hikariConfig()))

    private fun hikariConfig(): HikariConfig =
        HikariConfig().apply {
            jdbcUrl = dbJdbcUrl // "jdbc:postgresql://localhost:$dbPort/$dbName" // This is where the cloud db proxy is located in the pod
            driverClassName = "org.postgresql.Driver"
            minimumIdle = 1
            maxLifetime = 26000
            maximumPoolSize = 10
            connectionTimeout = 250
            idleTimeout = 10000
            isAutoCommit = false
            // Isolation level that ensure the same snapshot of db during one transaction:
            transactionIsolation = "TRANSACTION_REPEATABLE_READ"
        }

    fun createEnhetsregisterSnapshotTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $ENHETSREGISTER_SNAPSHOT" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $ENHETSREGISTER_SNAPSHOT", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $ENHETSREGISTER_SNAPSHOT" }
            SchemaUtils.create(EnhetsregisterSnapshotTable)
        }
    }

    fun createEnhetSnapshotTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $ENHET_SNAPSHOT" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $ENHET_SNAPSHOT", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $ENHET_SNAPSHOT" }
            SchemaUtils.create(EnhetSnapshotTable)
        }
    }

    fun createUnderenhetSnapshotTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $UNDERENHET_SNAPSHOT" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $UNDERENHET_SNAPSHOT", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $UNDERENHET_SNAPSHOT" }
            SchemaUtils.create(UnderenhetSnapshotTable)
        }
    }
}
