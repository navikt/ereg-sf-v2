import no.nav.ereg.Application
import no.nav.ereg.OrgType
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.date
import org.jetbrains.exposed.sql.javatime.timestamp
import java.time.Instant
import java.time.LocalDate

const val ENHETSREGISTER_SNAPSHOT = "enhetsregister_snapshot"
const val ENHET_SNAPSHOT = "enhet_snapshot"
const val UNDERENHET_SNAPSHOT = "underenhet_snapshot"
const val SALESFORCE_INITIAL_LOAD_PROGRESS = "salesforce_initial_load_progress"

object EnhetsregisterSnapshotTable : Table(ENHETSREGISTER_SNAPSHOT) {
    val snapshotDate = date("snapshot_date")
    val status = varchar("status", 20)
    val startedAt = timestamp("started_at")
    val completedAt = timestamp("completed_at").nullable()

    val enhetCount = integer("enhet_count").nullable()
    val underenhetCount = integer("underenhet_count").nullable()

    val sourceChecksum = varchar("source_checksum", 128).nullable()

    override val primaryKey =
        PrimaryKey(snapshotDate)
}

object EnhetSnapshotTable : Table(ENHET_SNAPSHOT) {
    val snapshotDate = date("snapshot_date")
    val orgNumber = varchar("org_number", 20)

    val name = varchar("name", 500).nullable()

    val registrationDate =
        date("registration_date").nullable()

    val payloadHash =
        varchar("payload_hash", 64)

    val payload =
        text("payload")

    override val primaryKey =
        PrimaryKey(
            snapshotDate,
            orgNumber,
        )
}

object UnderenhetSnapshotTable : Table(UNDERENHET_SNAPSHOT) {
    val snapshotDate = date("snapshot_date")
    val orgNumber = varchar("org_number", 20)

    val name = varchar("name", 500).nullable()

    val registrationDate =
        date("registration_date").nullable()

    val payloadHash =
        varchar("payload_hash", 64)

    val payload =
        text("payload")

    override val primaryKey =
        PrimaryKey(
            snapshotDate,
            orgNumber,
        )
}

object SalesforceInitialLoadProgressTable : Table(SALESFORCE_INITIAL_LOAD_PROGRESS) {
    val snapshotDate = date("snapshot_date")
    val orgType = varchar("org_type", 20)
    val status = varchar("status", 20)
    val lastOrgNumber = varchar("last_org_number", 20).nullable()
    val startedAt = timestamp("started_at")
    val completedAt = timestamp("completed_at").nullable()

    override val primaryKey =
        PrimaryKey(
            snapshotDate,
            orgType,
        )
}

data class EnhetsregisterSnapshot(
    val snapshotDate: LocalDate,
    val status: EnhetsregisterSnapshotStatus,
    val startedAt: Instant,
    val completedAt: Instant?,
    val enhetCount: Int?,
    val underenhetCount: Int?,
    val sourceChecksum: String?,
)

enum class EnhetsregisterSnapshotStatus {
    LOADING,
    READY,
    FAILED,
}

fun ResultRow.toEnhetsregisterSnapshot() =
    EnhetsregisterSnapshot(
        snapshotDate = this[EnhetsregisterSnapshotTable.snapshotDate],
        status =
            EnhetsregisterSnapshotStatus.valueOf(
                this[EnhetsregisterSnapshotTable.status],
            ),
        startedAt = this[EnhetsregisterSnapshotTable.startedAt],
        completedAt = this[EnhetsregisterSnapshotTable.completedAt],
        enhetCount = this[EnhetsregisterSnapshotTable.enhetCount],
        underenhetCount = this[EnhetsregisterSnapshotTable.underenhetCount],
        sourceChecksum = this[EnhetsregisterSnapshotTable.sourceChecksum],
    )

data class EnhetSnapshot(
    val snapshotDate: LocalDate,
    val orgNumber: String,
    val name: String?,
    val registrationDate: LocalDate?,
    val payloadHash: String,
    val payload: String,
)

fun ResultRow.toEnhetSnapshot() =
    EnhetSnapshot(
        snapshotDate = this[EnhetSnapshotTable.snapshotDate],
        orgNumber = this[EnhetSnapshotTable.orgNumber],
        name = this[EnhetSnapshotTable.name],
        registrationDate = this[EnhetSnapshotTable.registrationDate],
        payloadHash = this[EnhetSnapshotTable.payloadHash],
        payload = this[EnhetSnapshotTable.payload],
    )

data class UnderenhetSnapshot(
    val snapshotDate: LocalDate,
    val orgNumber: String,
    val name: String?,
    val registrationDate: LocalDate?,
    val payloadHash: String,
    val payload: String,
)

fun ResultRow.toUnderenhetSnapshot() =
    UnderenhetSnapshot(
        snapshotDate = this[UnderenhetSnapshotTable.snapshotDate],
        orgNumber = this[UnderenhetSnapshotTable.orgNumber],
        name = this[UnderenhetSnapshotTable.name],
        registrationDate = this[UnderenhetSnapshotTable.registrationDate],
        payloadHash = this[UnderenhetSnapshotTable.payloadHash],
        payload = this[UnderenhetSnapshotTable.payload],
    )

enum class SalesforceInitialLoadStatus {
    NOT_STARTED,
    IN_PROGRESS,
    FAILED,
    DONE,
}

data class SalesforceInitialLoadProgress(
    val snapshotDate: LocalDate,
    val orgType: OrgType,
    val status: SalesforceInitialLoadStatus,
    val lastOrgNumber: String?,
    val startedAt: Instant,
    val completedAt: Instant?,
)

fun ResultRow.toSalesforceInitialLoadProgress() =
    SalesforceInitialLoadProgress(
        snapshotDate =
            this[SalesforceInitialLoadProgressTable.snapshotDate],
        orgType =
            OrgType.valueOf(
                this[SalesforceInitialLoadProgressTable.orgType],
            ),
        status =
            SalesforceInitialLoadStatus.valueOf(
                this[SalesforceInitialLoadProgressTable.status],
            ),
        lastOrgNumber =
            this[SalesforceInitialLoadProgressTable.lastOrgNumber],
        startedAt =
            this[SalesforceInitialLoadProgressTable.startedAt],
        completedAt =
            this[SalesforceInitialLoadProgressTable.completedAt],
    )
