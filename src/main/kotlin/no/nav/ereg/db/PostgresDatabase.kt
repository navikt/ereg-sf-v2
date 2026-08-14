package no.nav.sf.keytool.db

import ENHETSREGISTER_SNAPSHOT
import ENHET_SNAPSHOT
import EnhetSnapshot
import EnhetSnapshotTable
import EnhetsregisterSnapshot
import EnhetsregisterSnapshotTable
import SALESFORCE_DIFF_PROGRESS
import SALESFORCE_INITIAL_LOAD_PROGRESS
import SalesforceDiffPhase
import SalesforceDiffProgress
import SalesforceDiffProgressTable
import SalesforceInitialLoadProgress
import SalesforceInitialLoadProgressTable
import UNDERENHET_SNAPSHOT
import UnderenhetSnapshot
import UnderenhetSnapshotTable
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import no.nav.ereg.OrgType
import no.nav.ereg.env
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.batchInsert
import org.jetbrains.exposed.sql.deleteWhere
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.update
import org.jetbrains.exposed.sql.upsert
import toEnhetSnapshot
import toEnhetsregisterSnapshot
import toSalesforceDiffProgress
import toSalesforceInitialLoadProgress
import toUnderenhetSnapshot
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

    fun createSalesforceInitialLoadProgressTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $SALESFORCE_INITIAL_LOAD_PROGRESS" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $SALESFORCE_INITIAL_LOAD_PROGRESS", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $SALESFORCE_INITIAL_LOAD_PROGRESS" }
            SchemaUtils.create(SalesforceInitialLoadProgressTable)
        }
    }

    fun createSalesforceDiffProgressTable(dropFirst: Boolean = false) {
        transaction {
            if (dropFirst) {
                log.info { "Dropping table $SALESFORCE_DIFF_PROGRESS" }
                val dropStatement =
                    TransactionManager.current().connection.prepareStatement("DROP TABLE $SALESFORCE_DIFF_PROGRESS", false)
                dropStatement.executeUpdate()
                log.info { "Drop performed" }
            }

            log.info { "Creating table $SALESFORCE_DIFF_PROGRESS" }
            SchemaUtils.create(SalesforceDiffProgressTable)
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

    fun getSalesforceInitialLoadProgress(
        snapshotDate: LocalDate,
        orgType: OrgType,
    ): SalesforceInitialLoadProgress? =
        transaction(database) {
            SalesforceInitialLoadProgressTable
                .selectAll()
                .where {
                    (SalesforceInitialLoadProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceInitialLoadProgressTable.orgType eq orgType.name)
                }.singleOrNull()
                ?.toSalesforceInitialLoadProgress()
        }

    fun startSalesforceInitialLoad(
        snapshotDate: LocalDate,
        orgType: OrgType,
        lastOrgNumber: String? = null,
    ) {
        transaction(database) {
            SalesforceInitialLoadProgressTable.upsert(
                keys =
                    arrayOf(
                        SalesforceInitialLoadProgressTable.snapshotDate,
                        SalesforceInitialLoadProgressTable.orgType,
                    ),
            ) {
                it[SalesforceInitialLoadProgressTable.snapshotDate] = snapshotDate
                it[SalesforceInitialLoadProgressTable.orgType] = orgType.name
                it[status] = SalesforceInitialLoadStatus.IN_PROGRESS.name
                it[SalesforceInitialLoadProgressTable.lastOrgNumber] = lastOrgNumber
                it[startedAt] = Instant.now()
                it[completedAt] = null
            }
        }
    }

    fun updateSalesforceInitialLoadProgress(
        snapshotDate: LocalDate,
        orgType: OrgType,
        lastOrgNumber: String,
    ) {
        transaction(database) {
            SalesforceInitialLoadProgressTable.update(
                where = {
                    (SalesforceInitialLoadProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceInitialLoadProgressTable.orgType eq orgType.name)
                },
            ) {
                it[SalesforceInitialLoadProgressTable.lastOrgNumber] =
                    lastOrgNumber

                it[status] =
                    SalesforceInitialLoadStatus.IN_PROGRESS.name
            }
        }
    }

    fun markSalesforceInitialLoadDone(
        snapshotDate: LocalDate,
        orgType: OrgType,
    ) {
        transaction(database) {
            SalesforceInitialLoadProgressTable.update(
                where = {
                    (SalesforceInitialLoadProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceInitialLoadProgressTable.orgType eq orgType.name)
                },
            ) {
                it[status] =
                    SalesforceInitialLoadStatus.DONE.name
                it[completedAt] = Instant.now()
            }
        }
    }

    fun markSalesforceInitialLoadFailed(
        snapshotDate: LocalDate,
        orgType: OrgType,
    ) {
        transaction(database) {
            SalesforceInitialLoadProgressTable.update(
                where = {
                    (SalesforceInitialLoadProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceInitialLoadProgressTable.orgType eq orgType.name)
                },
            ) {
                it[status] =
                    SalesforceInitialLoadStatus.FAILED.name
                it[completedAt] = Instant.now()
            }
        }
    }

    fun fetchEnhetBatch(
        snapshotDate: LocalDate,
        afterOrgNumber: String?,
        limit: Int = 100,
    ): List<EnhetSnapshot> =
        transaction(database) {
            val query =
                if (afterOrgNumber == null) {
                    EnhetSnapshotTable
                        .selectAll()
                        .where {
                            EnhetSnapshotTable.snapshotDate eq snapshotDate
                        }
                } else {
                    EnhetSnapshotTable
                        .selectAll()
                        .where {
                            (EnhetSnapshotTable.snapshotDate eq snapshotDate) and
                                (EnhetSnapshotTable.orgNumber greater afterOrgNumber)
                        }
                }

            query
                .orderBy(
                    EnhetSnapshotTable.orgNumber to SortOrder.ASC,
                ).limit(limit)
                .map { it.toEnhetSnapshot() }
        }

    fun fetchUnderenhetBatch(
        snapshotDate: LocalDate,
        afterOrgNumber: String?,
        limit: Int = 100,
    ): List<UnderenhetSnapshot> =
        transaction(database) {
            val query =
                if (afterOrgNumber == null) {
                    UnderenhetSnapshotTable
                        .selectAll()
                        .where {
                            UnderenhetSnapshotTable.snapshotDate eq snapshotDate
                        }
                } else {
                    UnderenhetSnapshotTable
                        .selectAll()
                        .where {
                            (UnderenhetSnapshotTable.snapshotDate eq snapshotDate) and
                                (UnderenhetSnapshotTable.orgNumber greater afterOrgNumber)
                        }
                }

            query
                .orderBy(
                    UnderenhetSnapshotTable.orgNumber to SortOrder.ASC,
                ).limit(limit)
                .map { it.toUnderenhetSnapshot() }
        }

    fun getSalesforceDiffProgress(
        snapshotDate: LocalDate,
        orgType: OrgType,
        phase: SalesforceDiffPhase,
    ): SalesforceDiffProgress? =
        transaction(database) {
            SalesforceDiffProgressTable
                .selectAll()
                .where {
                    (SalesforceDiffProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceDiffProgressTable.orgType eq orgType.name) and
                        (SalesforceDiffProgressTable.phase eq phase.name)
                }.singleOrNull()
                ?.toSalesforceDiffProgress()
        }

    fun startSalesforceDiff(
        snapshotDate: LocalDate,
        orgType: OrgType,
        phase: SalesforceDiffPhase,
        existing: SalesforceDiffProgress?,
    ) {
        if (
            existing?.status == SalesforceDiffStatus.IN_PROGRESS
        ) {
            return
        }

        transaction(database) {
            SalesforceDiffProgressTable.upsert(
                keys =
                    arrayOf(
                        SalesforceDiffProgressTable.snapshotDate,
                        SalesforceDiffProgressTable.orgType,
                        SalesforceDiffProgressTable.phase,
                    ),
            ) {
                it[SalesforceDiffProgressTable.snapshotDate] = snapshotDate
                it[SalesforceDiffProgressTable.orgType] = orgType.name
                it[SalesforceDiffProgressTable.phase] = phase.name
                it[status] = SalesforceDiffStatus.IN_PROGRESS.name

                it[lastOrgNumber] =
                    existing?.lastOrgNumber

                it[newCount] =
                    existing?.newCount ?: 0

                it[updatedCount] =
                    existing?.updatedCount ?: 0

                it[removedCount] =
                    existing?.removedCount ?: 0

                it[startedAt] =
                    existing?.startedAt ?: Instant.now()

                it[completedAt] = null
            }
        }
    }

    fun updateSalesforceDiffProgress(
        snapshotDate: LocalDate,
        orgType: OrgType,
        phase: SalesforceDiffPhase,
        lastOrgNumber: String,
        newCount: Int,
        updatedCount: Int,
        removedCount: Int,
    ) {
        transaction(database) {
            SalesforceDiffProgressTable.update(
                where = {
                    (SalesforceDiffProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceDiffProgressTable.orgType eq orgType.name) and
                        (SalesforceDiffProgressTable.phase eq phase.name)
                },
            ) {
                it[SalesforceDiffProgressTable.lastOrgNumber] = lastOrgNumber
                it[SalesforceDiffProgressTable.newCount] = newCount
                it[SalesforceDiffProgressTable.updatedCount] = updatedCount
                it[SalesforceDiffProgressTable.removedCount] = removedCount
            }
        }
    }

    fun markSalesforceDiffDone(
        snapshotDate: LocalDate,
        orgType: OrgType,
        phase: SalesforceDiffPhase,
    ) {
        transaction(database) {
            SalesforceDiffProgressTable.update(
                where = {
                    (SalesforceDiffProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceDiffProgressTable.orgType eq orgType.name) and
                        (SalesforceDiffProgressTable.phase eq phase.name)
                },
            ) {
                it[status] = SalesforceDiffStatus.DONE.name
                it[completedAt] = Instant.now()
            }
        }
    }

    fun markSalesforceDiffFailed(
        snapshotDate: LocalDate,
        orgType: OrgType,
        phase: SalesforceDiffPhase,
    ) {
        transaction(database) {
            SalesforceDiffProgressTable.update(
                where = {
                    (SalesforceDiffProgressTable.snapshotDate eq snapshotDate) and
                        (SalesforceDiffProgressTable.orgType eq orgType.name) and
                        (SalesforceDiffProgressTable.phase eq phase.name)
                },
            ) {
                it[status] = SalesforceDiffStatus.FAILED.name
                it[completedAt] = Instant.now()
            }
        }
    }

    fun fetchEnhetRows(
        snapshotDate: LocalDate,
        orgNumbers: List<String>,
    ): List<EnhetSnapshot> =
        if (orgNumbers.isEmpty()) {
            emptyList()
        } else {
            transaction(database) {
                EnhetSnapshotTable
                    .selectAll()
                    .where {
                        (EnhetSnapshotTable.snapshotDate eq snapshotDate) and
                            (EnhetSnapshotTable.orgNumber inList orgNumbers)
                    }.map { it.toEnhetSnapshot() }
            }
        }

    fun fetchUnderenhetRows(
        snapshotDate: LocalDate,
        orgNumbers: List<String>,
    ): List<UnderenhetSnapshot> =
        if (orgNumbers.isEmpty()) {
            emptyList()
        } else {
            transaction(database) {
                UnderenhetSnapshotTable
                    .selectAll()
                    .where {
                        (UnderenhetSnapshotTable.snapshotDate eq snapshotDate) and
                            (UnderenhetSnapshotTable.orgNumber inList orgNumbers)
                    }.map { it.toUnderenhetSnapshot() }
            }
        }
}
