package no.nav.sf.keytool.db

import ENHETSREGISTER_SNAPSHOT
import ENHET_SNAPSHOT
import EnhetSnapshot
import EnhetSnapshotTable
import EnhetsregisterSnapshot
import EnhetsregisterSnapshotTable
import UNDERENHET_SNAPSHOT
import UnderenhetSnapshot
import UnderenhetSnapshotTable
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import no.nav.ereg.env
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import toEnhetsregisterSnapshot
import java.time.Instant
import java.time.LocalDate

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

    fun createEnhetsregisterSnapshot(snapshotDate: LocalDate) {
        transaction(database) {
            EnhetsregisterSnapshotTable.insert {
                it[EnhetsregisterSnapshotTable.snapshotDate] = snapshotDate
                it[EnhetsregisterSnapshotTable.status] =
                    EnhetsregisterSnapshotStatus.LOADING.name
                it[EnhetsregisterSnapshotTable.startedAt] = Instant.now()
                it[EnhetsregisterSnapshotTable.completedAt] = null
                it[EnhetsregisterSnapshotTable.enhetCount] = null
                it[EnhetsregisterSnapshotTable.underenhetCount] = null
                it[EnhetsregisterSnapshotTable.sourceChecksum] = null
            }
        }
    }

    fun saveEnhetBatch(
        snapshotDate: LocalDate,
        rows: Collection<EnhetSnapshot>,
    ) {
        if (rows.isEmpty()) return

        transaction(database) {
            EnhetSnapshotTable.batchInsert(rows) { row ->
                this[EnhetSnapshotTable.snapshotDate] = row.snapshotDate
                this[EnhetSnapshotTable.orgNumber] = row.orgNumber
                this[EnhetSnapshotTable.name] = row.name
                this[EnhetSnapshotTable.registrationDate] = row.registrationDate
                this[EnhetSnapshotTable.payloadHash] = row.payloadHash
                this[EnhetSnapshotTable.payload] = row.payload
            }
        }
    }

    fun saveUnderenhetBatch(
        snapshotDate: LocalDate,
        rows: Collection<UnderenhetSnapshot>,
    ) {
        if (rows.isEmpty()) return

        transaction(database) {
            UnderenhetSnapshotTable.batchInsert(rows) { row ->
                this[UnderenhetSnapshotTable.snapshotDate] = row.snapshotDate
                this[UnderenhetSnapshotTable.orgNumber] = row.orgNumber
                this[UnderenhetSnapshotTable.name] = row.name
                this[UnderenhetSnapshotTable.registrationDate] = row.registrationDate
                this[UnderenhetSnapshotTable.payloadHash] = row.payloadHash
                this[UnderenhetSnapshotTable.payload] = row.payload
            }
        }
    }

    fun markSnapshotReady(
        snapshotDate: LocalDate,
        enhetCount: Int,
        underenhetCount: Int,
        sourceChecksum: String?,
    ) {
        transaction(database) {
            EnhetsregisterSnapshotTable.update(
                where = {
                    EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
                },
            ) {
                it[status] = EnhetsregisterSnapshotStatus.READY.name
                it[completedAt] = Instant.now()
                it[EnhetsregisterSnapshotTable.enhetCount] = enhetCount
                it[EnhetsregisterSnapshotTable.underenhetCount] = underenhetCount
                it[EnhetsregisterSnapshotTable.sourceChecksum] = sourceChecksum
            }
        }
    }

    fun markSnapshotFailed(snapshotDate: LocalDate) {
        transaction(database) {
            EnhetsregisterSnapshotTable.update(
                where = {
                    EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
                },
            ) {
                it[status] = EnhetsregisterSnapshotStatus.FAILED.name
                it[completedAt] = Instant.now()
            }
        }
    }

    fun isReadySnapshot(snapshotDate: LocalDate): Boolean =
        transaction(database) {
            EnhetsregisterSnapshotTable
                .select(
                    EnhetsregisterSnapshotTable.status,
                ).where {
                    (EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate) and
                        (
                            EnhetsregisterSnapshotTable.status eq
                                EnhetsregisterSnapshotStatus.READY.name
                        )
                }.limit(1)
                .count() == 1L
        }

    fun getEnhetsregisterSnapshot(snapshotDate: LocalDate): EnhetsregisterSnapshot? =
        transaction(database) {
            EnhetsregisterSnapshotTable
                .selectAll()
                .where {
                    EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
                }.singleOrNull()
                ?.toEnhetsregisterSnapshot()
        }

    fun updateEnhetSnapshotProgress(
        snapshotDate: LocalDate,
        enhetCount: Int,
    ) {
        transaction(database) {
            EnhetsregisterSnapshotTable.update(
                where = {
                    EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
                },
            ) {
                it[EnhetsregisterSnapshotTable.enhetCount] = enhetCount
            }
        }
    }

    fun deleteEnhetsregisterSnapshot(snapshotDate: LocalDate) {
        transaction(database) {
            EnhetSnapshotTable.deleteWhere {
                EnhetSnapshotTable.snapshotDate eq snapshotDate
            }

            EnhetsregisterSnapshotTable.deleteWhere {
                EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
            }
        }
    }

    fun updateUnderenhetSnapshotProgress(
        snapshotDate: LocalDate,
        underenhetCount: Int,
    ) {
        transaction(database) {
            EnhetsregisterSnapshotTable.update(
                where = {
                    EnhetsregisterSnapshotTable.snapshotDate eq snapshotDate
                },
            ) {
                it[EnhetsregisterSnapshotTable.underenhetCount] =
                    underenhetCount
            }
        }
    }
}
