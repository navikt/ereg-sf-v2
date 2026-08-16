@file:Suppress("ktlint:standard:property-naming")

package no.nav.ereg

import EnhetSnapshot
import UnderenhetSnapshot
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import filesHandler
import mu.KotlinLogging
import no.nav.ereg.salesforce.KafkaMessage
import no.nav.ereg.salesforce.SalesforceClient
import no.nav.ereg.salesforce.encodeB64
import no.nav.ereg.token.AuthRouteBuilder
import no.nav.ereg.token.DefaultTokenValidator
import no.nav.ereg.token.MockTokenValidator
import no.nav.sf.keytool.db.PostgresDatabase
import no.nav.sf.keytool.db.PostgresDatabase.startSalesforceInitialLoad
import okhttp3.OkHttpClient
import org.http4k.client.OkHttp
import org.http4k.core.BodyMode
import org.http4k.core.Filter
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.core.Status.Companion.OK
import org.http4k.filter.gunzippedStream
import org.http4k.routing.ResourceLoader
import org.http4k.routing.bind
import org.http4k.routing.path
import org.http4k.routing.routes
import org.http4k.routing.static
import org.http4k.server.Http4kServer
import org.http4k.server.Netty
import org.http4k.server.asServer
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.security.MessageDigest
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

const val PARSE_TO_DB_BATCH_SIZE = 2_000
const val DIFF_BATCH_SIZE = 2_000
const val ENHETER_URL =
    "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"

const val ENHET_ACCEPT_HEADER =
    "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"

const val UNDERENHETER_URL =
    "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned"

const val UNDERENHET_ACCEPT_HEADER =
    "application/vnd.brreg.enhetsregisteret.underenhet.v2+gzip;charset=UTF-8"

const val orgTopic = "public-ereg-cache-org-json"
const val tombstoneTopic = "public-ereg-cache-org-tombstones"

class Application {
    private val log = KotlinLogging.logger { }

    val gson = Gson()

    val local: Boolean = System.getenv(env_NAIS_CLUSTER_NAME) == null

    val tokenValidator = if (local) MockTokenValidator() else DefaultTokenValidator()

    val cluster = if (local) "local" else env(env_NAIS_CLUSTER_NAME)

    val salesforceClient = SalesforceClient()

    private val salesforceFullLoadRunning = AtomicBoolean(false)

    private val diffRunning = AtomicBoolean(false)

    private val okHttpClient =
        OkHttp(
            client =
                OkHttpClient
                    .Builder()
                    .connectTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(10, TimeUnit.MINUTES)
                    .writeTimeout(30, TimeUnit.SECONDS)
                    .callTimeout(60, TimeUnit.MINUTES)
                    .build(),
            bodyMode = BodyMode.Stream,
        )

    fun apiServer(port: Int): Http4kServer = api().asServer(Netty(port))

    fun api(): HttpHandler =
        // accessLog(
        routes(
            "/internal/isAlive" bind Method.GET to { Response(OK) },
            "/internal/isReady" bind Method.GET to { Response(OK) },
            "/internal/metrics" bind Method.GET to Metrics.metricsHttpHandler,
            "/internal/gui" bind Method.GET to static(ResourceLoader.Classpath("gui")),
            "/internal/gui/api/org/{orgNumber}" bind Method.GET to organisationLookupHandler,
            "/internal/hello" bind Method.GET to { Response(OK).body("Hello") },
            "/internal/secrethello" authbind Method.GET to { Response(OK).body("Secret Hello") },
            "/internal/files" bind Method.GET to filesHandler(File("/tmp/files")),
            "/internal/files/{path:.*}" bind Method.GET to filesHandler(File("/tmp/files")),
            "/internal/clearDb" bind Method.GET to clearDbHandler,
            "/internal/initDb" bind Method.GET to initDbHandler,
            "/internal/triggerRun" bind Method.GET to triggerRunHandler,
            "/internal/status" bind Method.GET to { runResponse(LocalDate.now()) },
            "/internal/statusYesterday" bind Method.GET to { runResponse(LocalDate.now().minusDays(1)) },
            "/internal/salesforce/fullLoad" bind Method.GET to triggerSalesforceFullLoadHandler,
            "/internal/salesforce/testLoad" bind Method.GET to testSending5EnhetAnd5Underenhet,
            "/internal/sendTodayChanges" bind Method.GET to sendTodayChangesHandler,
            "/internal/gui/api/diff-organisations" bind Method.GET to diffOrganisationsHandler,
            "/internal/databaseDiagnostics" bind Method.GET to databaseDiagnosticsHandler,
        )
    // )

    /**
     * authbind: a variant of bind that takes care of authentication with use of tokenValidator
     */
    infix fun String.authbind(method: Method) = AuthRouteBuilder(this, method, tokenValidator)

    fun start() {
        log.info { "Starting in cluster $cluster" }
        val dir = File("/tmp/files")
        dir.mkdirs() // ensures /tmp/files exists
        apiServer(8080).start()
        recoverAfterStartup() // Check on status of today if there is one in progress and resume in that case
    }

    private val diffOrganisationsHandler: HttpHandler = { request ->

        val date =
            request
                .query("date")
                ?.let {
                    runCatching {
                        LocalDate.parse(it)
                    }.getOrNull()
                }

        val orgType =
            request
                .query("type")
                ?.let {
                    runCatching {
                        OrgType.valueOf(it.uppercase())
                    }.getOrNull()
                }

        val changeType =
            request
                .query("changeType")
                ?.let {
                    runCatching {
                        ChangeType.valueOf(it.uppercase())
                    }.getOrNull()
                }

        when {
            date == null ->
                Response(Status.BAD_REQUEST)
                    .body("Invalid or missing date. Expected yyyy-MM-dd")

            orgType == null ->
                Response(Status.BAD_REQUEST)
                    .body("Invalid or missing type. Expected ENHET or UNDERENHET")

            changeType == null ->
                Response(Status.BAD_REQUEST)
                    .body(
                        "Invalid or missing changeType. " +
                            "Expected NEW, UPDATED or REMOVED",
                    )

            else -> {
                val rows =
                    PostgresDatabase.getSalesforceDiffOrganisations(
                        snapshotDate = date,
                        orgType = orgType,
                        changeType = changeType,
                        limit = 500,
                    )

                Response(Status.OK)
                    .header("Content-Type", "application/json")
                    .body(gson.toJson(rows))
            }
        }
    }

    private val organisationLookupHandler: HttpHandler = { request ->
        val orgNumber =
            request.path("orgNumber")

        val date =
            request
                .query("date")
                ?.let {
                    runCatching { LocalDate.parse(it) }.getOrNull()
                }

        when {
            orgNumber.isNullOrBlank() ->
                Response(Status.BAD_REQUEST)
                    .body("Missing orgNumber")

            date == null ->
                Response(Status.BAD_REQUEST)
                    .body("Missing or invalid date. Expected yyyy-MM-dd")

            else -> {
                val enhet =
                    PostgresDatabase.getEnhetSnapshot(
                        snapshotDate = date,
                        orgNumber = orgNumber,
                    )

                val underenhet =
                    PostgresDatabase.getUnderenhetSnapshot(
                        snapshotDate = date,
                        orgNumber = orgNumber,
                    )

                if (enhet == null && underenhet == null) {
                    Response(Status.NOT_FOUND)
                        .body(
                            "Organisation $orgNumber not found in snapshot $date",
                        )
                } else {
                    val response =
                        OrganisationSnapshotResponse(
                            orgNumber = orgNumber,
                            snapshots =
                                listOfNotNull(
                                    enhet?.let {
                                        OrganisationSnapshot(
                                            orgType = OrgType.ENHET.name,
                                            name = it.name,
                                            registrationDate =
                                                it.registrationDate?.toString(),
                                            payload = it.payload,
                                        )
                                    },
                                    underenhet?.let {
                                        OrganisationSnapshot(
                                            orgType = OrgType.UNDERENHET.name,
                                            name = it.name,
                                            registrationDate =
                                                it.registrationDate?.toString(),
                                            payload = it.payload,
                                        )
                                    },
                                ),
                        )

                    Response(Status.OK)
                        .header(
                            "Content-Type",
                            "application/json",
                        ).body(
                            gson.toJson(response),
                        )
                }
            }
        }
    }

    private fun sendTodayChanges(today: LocalDate) {
        val yesterday = today.minusDays(1)

        val todayRun = fetchRun(today)
        val yesterdayRun = fetchRun(yesterday)

        require(todayRun.status == TodayRunStatus.DONE) {
            "Today's snapshot is not DONE"
        }

        require(yesterdayRun.status == TodayRunStatus.DONE) {
            "Yesterday's snapshot is not DONE"
        }

        PostgresDatabase.resetSalesforceDiff(
            snapshotDate = today,
        )

        val result =
            runSalesforceDiff(
                today = today,
                yesterday = yesterday,
            )

        log.info {
            "Manual Salesforce send: " +
                "total=${result.total}, " +
                "ENHET new=${result.enhet.new}, " +
                "updated=${result.enhet.updated}, " +
                "removed=${result.enhet.removed}, " +
                "UNDERENHET new=${result.underenhet.new}, " +
                "updated=${result.underenhet.updated}, " +
                "removed=${result.underenhet.removed}"
        }

        if (result.changes.isNotEmpty()) {
            salesforceClient.postChanges(result.changes)
        }
    }

    private val sendTodayChangesHandler: HttpHandler = {
        val today = LocalDate.now()

        if (!diffRunning.compareAndSet(false, true)) {
            Response(Status.CONFLICT)
                .body("Diff/send is already running")
        } else {
            runExecutor.submit {
                try {
                    sendTodayChanges(today)
                } catch (e: Exception) {
                    log.error(e) {
                        "Sending today's changes to Salesforce failed"
                    }
                } finally {
                    diffRunning.set(false)
                }
            }

            Response(Status.ACCEPTED)
                .body("Today's changes are being sent to Salesforce")
        }
    }

    private val clearDbHandler: HttpHandler = {
        PostgresDatabase.createEnhetsregisterSnapshotTable(true)
        PostgresDatabase.createEnhetSnapshotTable(true)
        PostgresDatabase.createUnderenhetSnapshotTable(true)
        PostgresDatabase.createSalesforceInitialLoadProgressTable(true)
        PostgresDatabase.createSalesforceDiffProgressTable(true)
        PostgresDatabase.createSalesforceDiffOrganisationTable(true)
        Response(OK).body("Tables recreated")
    }

    private val initDbHandler: HttpHandler = {
//        PostgresDatabase.createEnhetsregisterSnapshotTable(false)
//        PostgresDatabase.createEnhetSnapshotTable(false)
//        PostgresDatabase.createUnderenhetSnapshotTable(false)
//        PostgresDatabase.createSalesforceInitialLoadProgressTable(false)
//        PostgresDatabase.createSalesforceDiffProgressTable(false)
        PostgresDatabase.createSalesforceDiffOrganisationTable(false)
        Response(OK).body("Table created!")
    }

    private fun extractName(json: String): String? =
        runCatching {
            JsonParser
                .parseString(json)
                .asJsonObject
                .get("navn")
                ?.takeIf { !it.isJsonNull }
                ?.asString
        }.getOrNull()

    private fun importEnheter(
        input: InputStream,
        snapshotDate: LocalDate,
    ) {
        val batch = ArrayList<EnhetSnapshot>(PARSE_TO_DB_BATCH_SIZE)
        var count = 0

        JsonReader(
            InputStreamReader(
                input,
                Charsets.UTF_8,
            ),
        ).use { reader ->

            reader.beginArray()

            while (reader.hasNext()) {
                val jsonObject =
                    JsonParser
                        .parseReader(reader)
                        .asJsonObject

                val json = jsonObject.toString()

                val orgNumber =
                    jsonObject
                        .get("organisasjonsnummer")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString
                        ?: error("Missing organisasjonsnummer")

                val name =
                    jsonObject
                        .get("navn")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString

                val registrationDate =
                    jsonObject
                        .get("registreringsdatoEnhetsregisteret")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString
                        ?.takeIf { it.isNotBlank() }
                        ?.let(LocalDate::parse)

                batch +=
                    EnhetSnapshot(
                        snapshotDate = snapshotDate,
                        orgNumber = orgNumber,
                        name = name,
                        registrationDate = registrationDate,
                        payloadHash = sha256(json),
                        payload = json,
                    )

                count++

                if (batch.size >= PARSE_TO_DB_BATCH_SIZE) {
                    PostgresDatabase.saveEnhetBatch(
                        snapshotDate = snapshotDate,
                        rows = batch,
                    )

                    batch.clear()

                    PostgresDatabase.updateEnhetSnapshotProgress(
                        snapshotDate = snapshotDate,
                        enhetCount = count,
                    )
                }
            }

            reader.endArray()
        }

        if (batch.isNotEmpty()) {
            PostgresDatabase.saveEnhetBatch(
                snapshotDate = snapshotDate,
                rows = batch,
            )

            PostgresDatabase.updateEnhetSnapshotProgress(
                snapshotDate = snapshotDate,
                enhetCount = count,
            )
        }
    }

    private fun sha256(value: String): String =
        MessageDigest
            .getInstance("SHA-256")
            .digest(value.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    private fun importUnderenheter(
        input: InputStream,
        snapshotDate: LocalDate,
    ) {
        val batch =
            ArrayList<UnderenhetSnapshot>(PARSE_TO_DB_BATCH_SIZE)

        var count = 0

        JsonReader(
            InputStreamReader(
                input,
                Charsets.UTF_8,
            ),
        ).use { reader ->

            reader.beginArray()

            while (reader.hasNext()) {
                val jsonObject =
                    JsonParser
                        .parseReader(reader)
                        .asJsonObject

                val json = jsonObject.toString()

                val orgNumber =
                    jsonObject
                        .get("organisasjonsnummer")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString
                        ?: error("Missing organisasjonsnummer")

                val name =
                    jsonObject
                        .get("navn")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString

                val registrationDate =
                    jsonObject
                        .get("registreringsdatoEnhetsregisteret")
                        ?.takeIf { !it.isJsonNull }
                        ?.asString
                        ?.takeIf { it.isNotBlank() }
                        ?.let(LocalDate::parse)

                batch +=
                    UnderenhetSnapshot(
                        snapshotDate = snapshotDate,
                        orgNumber = orgNumber,
                        name = name,
                        registrationDate = registrationDate,
                        payloadHash = sha256(json),
                        payload = json,
                    )

                count++

                if (batch.size >= PARSE_TO_DB_BATCH_SIZE) {
                    PostgresDatabase.saveUnderenhetBatch(
                        snapshotDate = snapshotDate,
                        rows = batch,
                    )

                    batch.clear()

                    PostgresDatabase.updateUnderenhetSnapshotProgress(
                        snapshotDate = snapshotDate,
                        underenhetCount = count,
                    )

                    if (count % 100_000 == 0) {
                        log.info {
                            "Imported $count UNDERENHET rows"
                        }
                    }
                }
            }

            reader.endArray()
        }

        if (batch.isNotEmpty()) {
            PostgresDatabase.saveUnderenhetBatch(
                snapshotDate = snapshotDate,
                rows = batch,
            )

            PostgresDatabase.updateUnderenhetSnapshotProgress(
                snapshotDate = snapshotDate,
                underenhetCount = count,
            )
        }

        log.info {
            "Finished importing $count UNDERENHET rows"
        }
    }

    private fun downloadAndImport(
        url: String,
        acceptHeader: String,
        snapshotDate: LocalDate,
        importer: (InputStream, LocalDate) -> Unit,
    ) {
        val request =
            Request(
                Method.GET,
                url,
            ).header(
                "Accept",
                acceptHeader,
            )

        val response = okHttpClient(request)

        if (!response.status.successful) {
            error(
                "Brreg download failed: " +
                    "${response.status} ${response.bodyString()}",
            )
        }

        response.body
            .gunzippedStream(
                maxSize = 10L * 1024 * 1024 * 1024,
            ).stream
            .use { stream ->
                importer(stream, snapshotDate)
            }
    }

    private fun downloadAndImportEnheter(snapshotDate: LocalDate) {
        downloadAndImport(
            url = ENHETER_URL,
            acceptHeader = ENHET_ACCEPT_HEADER,
            snapshotDate = snapshotDate,
            importer = ::importEnheter,
        )
    }

    private fun downloadAndImportUnderenheter(snapshotDate: LocalDate) {
        downloadAndImport(
            url = UNDERENHETER_URL,
            acceptHeader = UNDERENHET_ACCEPT_HEADER,
            snapshotDate = snapshotDate,
            importer = ::importUnderenheter,
        )
    }

    enum class TodayRunStatus {
        NOT_RUN,
        IN_PROGRESS,
        FAILED,
        DONE,
    }

    data class Run(
        val status: TodayRunStatus,
        val enhetCount: Int,
        val underenhetCount: Int,
    )

    fun fetchRun(date: LocalDate): Run {
        val snapshot =
            PostgresDatabase.getEnhetsregisterSnapshot(
                date,
            )
                ?: return Run(
                    status = TodayRunStatus.NOT_RUN,
                    enhetCount = 0,
                    underenhetCount = 0,
                )

        return Run(
            status =
                when (
                    snapshot.status
                ) {
                    EnhetsregisterSnapshotStatus.LOADING ->
                        TodayRunStatus.IN_PROGRESS

                    EnhetsregisterSnapshotStatus.FAILED ->
                        TodayRunStatus.FAILED

                    EnhetsregisterSnapshotStatus.READY ->
                        TodayRunStatus.DONE
                },
            enhetCount = snapshot.enhetCount ?: 0,
            underenhetCount = snapshot.underenhetCount ?: 0,
        )
    }

    fun runResponse(date: LocalDate): Response {
        val run = fetchRun(date)

        val enhetInitialLoad =
            PostgresDatabase.getSalesforceInitialLoadProgress(
                snapshotDate = date,
                orgType = OrgType.ENHET,
            )

        val underenhetInitialLoad =
            PostgresDatabase.getSalesforceInitialLoadProgress(
                snapshotDate = date,
                orgType = OrgType.UNDERENHET,
            )

        return Response(OK)
            .header("Content-Type", "application/json")
            .body(
                gson.toJson(
                    mapOf(
                        "date" to date.toString(),
                        "status" to run.status.name,
                        "enhetCount" to run.enhetCount,
                        "underenhetCount" to run.underenhetCount,
                        "salesforceInitialLoad" to
                            mapOf(
                                "enhet" to
                                    enhetInitialLoad?.let {
                                        mapOf(
                                            "status" to it.status.name,
                                            "lastOrgNumber" to it.lastOrgNumber,
                                            "completedAt" to it.completedAt?.toString(),
                                        )
                                    },
                                "underenhet" to
                                    underenhetInitialLoad?.let {
                                        mapOf(
                                            "status" to it.status.name,
                                            "lastOrgNumber" to it.lastOrgNumber,
                                            "completedAt" to it.completedAt?.toString(),
                                        )
                                    },
                            ),
                        "salesforceDiff" to diffProgress(date),
                    ),
                ),
            )
    }

    private val runExecutor =
        Executors.newSingleThreadExecutor {
            Thread(it, "enhetsregister-run").apply {
                isDaemon = true
            }
        }

    private val runLock = Any()

    private val triggerRunHandler: HttpHandler = {
        val today = LocalDate.now()

        synchronized(runLock) {
            val existing =
                PostgresDatabase.getEnhetsregisterSnapshot(today)

            when (existing?.status) {
                EnhetsregisterSnapshotStatus.LOADING -> {
                    Response(Status.CONFLICT)
                        .body("Today's snapshot is already loading")
                }

                EnhetsregisterSnapshotStatus.READY -> {
                    if (isTodayRunComplete(today)) {
                        Response(Status.OK)
                            .body(
                                "Today's run has already completed successfully",
                            )
                    } else {
                        startTodayRun(today)

                        Response(Status.ACCEPTED)
                            .body(
                                "Today's run started from existing snapshot",
                            )
                    }
                }

                EnhetsregisterSnapshotStatus.FAILED -> {
                    PostgresDatabase.resetSnapshot(today)
                    startTodayRun(today)

                    Response(Status.ACCEPTED)
                        .body("Today's run restarted")
                }

                null -> {
                    startTodayRun(today)

                    Response(Status.ACCEPTED)
                        .body("Run started")
                }
            }
        }
    }

    private fun startTodayRun(snapshotDate: LocalDate) {
        Metrics.publishedOrgs.clear()

        runExecutor.submit {
            try {
                runToday(snapshotDate)
            } catch (e: Exception) {
                log.error(e) {
                    "Daily run failed for $snapshotDate"
                }
            }
        }
    }

    private fun runToday(today: LocalDate) {
        val yesterday = today.minusDays(1)

        ensureTodaySnapshot(today)

        val todayRun = fetchRun(today)
        val yesterdayRun = fetchRun(yesterday)

        if (todayRun.status != TodayRunStatus.DONE) {
            log.error {
                "Today's snapshot is not DONE after snapshot phase: " +
                    todayRun.status
            }
            return
        }

        if (yesterdayRun.status != TodayRunStatus.DONE) {
            log.warn {
                "Yesterday's snapshot is not DONE. " +
                    "Skipping diff. status=${yesterdayRun.status}"
            }
            return
        }

        log.info {
            "Starting Salesforce diff $yesterday -> $today"
        }

        val result =
            runSalesforceDiff(
                today = today,
                yesterday = yesterday,
            )

        log.info {
            "Salesforce diff completed: " +
                "ENHET new=${result.enhet.new}, " +
                "updated=${result.enhet.updated}, " +
                "removed=${result.enhet.removed}; " +
                "UNDERENHET new=${result.underenhet.new}, " +
                "updated=${result.underenhet.updated}, " +
                "removed=${result.underenhet.removed}; " +
                "total=${result.total}"
        }

        if (result.changes.isNotEmpty()) {
            salesforceClient.postChanges(result.changes)
        }

        log.info {
            "Salesforce posting completed successfully"
        }

        PostgresDatabase.cleanupOldData(
            keepSnapshotDates = setOf(today, yesterday),
            keepStatusSince = today.minusDays(30),
        )

        log.info {
            "Daily run completed successfully for $today"
        }
    }

    private fun ensureTodaySnapshot(snapshotDate: LocalDate) {
        val existing =
            fetchRun(snapshotDate)

        when (existing.status) {
            TodayRunStatus.DONE -> {
                log.info {
                    "Snapshot $snapshotDate already READY"
                }
                return
            }

            TodayRunStatus.IN_PROGRESS ->
                error(
                    "Snapshot $snapshotDate is already in progress",
                )

            TodayRunStatus.FAILED -> {
                log.info {
                    "Removing failed snapshot $snapshotDate"
                }

                PostgresDatabase.deleteEnhetsregisterSnapshot(
                    snapshotDate,
                )

                PostgresDatabase.createEnhetsregisterSnapshot(
                    snapshotDate,
                )
            }

            TodayRunStatus.NOT_RUN -> {
                PostgresDatabase.createEnhetsregisterSnapshot(
                    snapshotDate,
                )
            }
        }

        try {
            log.info {
                "Starting Enhetsregister snapshot for $snapshotDate"
            }

            downloadAndImportEnheter(snapshotDate)

            downloadAndImportUnderenheter(snapshotDate)

            val snapshot =
                PostgresDatabase.getEnhetsregisterSnapshot(
                    snapshotDate,
                )
                    ?: error(
                        "Snapshot disappeared during run",
                    )

            PostgresDatabase.markSnapshotReady(
                snapshotDate = snapshotDate,
                enhetCount = snapshot.enhetCount ?: 0,
                underenhetCount = snapshot.underenhetCount ?: 0,
                sourceChecksum = null,
            )

            log.info {
                "Enhetsregister snapshot completed for $snapshotDate: " +
                    "ENHET=${snapshot.enhetCount ?: 0}, " +
                    "UNDERENHET=${snapshot.underenhetCount ?: 0}"
            }
        } catch (e: Exception) {
            log.error(e) {
                "Enhetsregister snapshot failed for $snapshotDate"
            }

            PostgresDatabase.markSnapshotFailed(
                snapshotDate,
            )

            throw e
        }
    }

    private fun isTodayRunComplete(today: LocalDate): Boolean =
        OrgType.entries.all { orgType ->
            SalesforceDiffPhase.entries.all { phase ->
                PostgresDatabase
                    .getSalesforceDiffProgress(
                        today,
                        orgType,
                        phase,
                    )?.status == SalesforceDiffStatus.DONE
            }
        }

    private fun runSalesforceFullLoad(snapshotDate: LocalDate) {
        log.info {
            "Starting Salesforce full load for $snapshotDate"
        }

        runSalesforceInitialLoadForEnhet(snapshotDate)
        runSalesforceInitialLoadForUnderenhet(snapshotDate)

        log.info {
            "Salesforce full load completed for $snapshotDate"
        }
    }

    private fun runSalesforceInitialLoadForEnhet(snapshotDate: LocalDate) {
        try {
            val existing =
                PostgresDatabase.getSalesforceInitialLoadProgress(
                    snapshotDate,
                    OrgType.ENHET,
                )

            var lastOrgNumber =
                existing?.lastOrgNumber

            PostgresDatabase.startSalesforceInitialLoad(
                snapshotDate = snapshotDate,
                orgType = OrgType.ENHET,
                lastOrgNumber = lastOrgNumber,
            )

            while (true) {
                val batch =
                    PostgresDatabase.fetchEnhetBatch(
                        snapshotDate = snapshotDate,
                        afterOrgNumber = lastOrgNumber,
                        limit = 100,
                    )

                if (batch.isEmpty()) {
                    PostgresDatabase.markSalesforceInitialLoadDone(
                        snapshotDate,
                        OrgType.ENHET,
                    )

                    return
                }

                val changes =
                    batch.map { it.toNewChange() }

                salesforceClient.postChanges(changes)

                lastOrgNumber =
                    batch.last().orgNumber

                PostgresDatabase.updateSalesforceInitialLoadProgress(
                    snapshotDate = snapshotDate,
                    orgType = OrgType.ENHET,
                    lastOrgNumber = lastOrgNumber,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceInitialLoadFailed(
                snapshotDate,
                OrgType.ENHET,
            )
            throw e
        }
    }

    private fun runSalesforceInitialLoadForUnderenhet(snapshotDate: LocalDate) {
        try {
            val existing =
                PostgresDatabase.getSalesforceInitialLoadProgress(
                    snapshotDate,
                    OrgType.UNDERENHET,
                )

            var lastOrgNumber =
                existing?.lastOrgNumber

            PostgresDatabase.startSalesforceInitialLoad(
                snapshotDate = snapshotDate,
                orgType = OrgType.UNDERENHET,
                lastOrgNumber = lastOrgNumber,
            )

            while (true) {
                val batch =
                    PostgresDatabase.fetchUnderenhetBatch(
                        snapshotDate = snapshotDate,
                        afterOrgNumber = lastOrgNumber,
                        limit = 100,
                    )

                if (batch.isEmpty()) {
                    PostgresDatabase.markSalesforceInitialLoadDone(
                        snapshotDate,
                        OrgType.UNDERENHET,
                    )

                    return
                }

                val changes =
                    batch.map { it.toNewChange() }

                salesforceClient.postChanges(changes)

                lastOrgNumber =
                    batch.last().orgNumber

                PostgresDatabase.updateSalesforceInitialLoadProgress(
                    snapshotDate = snapshotDate,
                    orgType = OrgType.UNDERENHET,
                    lastOrgNumber = lastOrgNumber,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceInitialLoadFailed(
                snapshotDate,
                OrgType.ENHET,
            )
            throw e
        }
    }

    val triggerSalesforceFullLoadHandler: HttpHandler = {
        val today = LocalDate.now()

        val snapshot =
            PostgresDatabase.getEnhetsregisterSnapshot(today)

        when {
            snapshot == null ->
                Response(Status.CONFLICT)
                    .body("Today's snapshot does not exist")

            snapshot.status != EnhetsregisterSnapshotStatus.READY ->
                Response(Status.CONFLICT)
                    .body(
                        "Today's snapshot is not READY: " +
                            snapshot.status,
                    )

            !salesforceFullLoadRunning.compareAndSet(
                false,
                true,
            ) ->
                Response(Status.CONFLICT)
                    .body("Salesforce full load already running")

            else -> {
                runExecutor.submit {
                    try {
                        runSalesforceFullLoad(today)
                    } catch (e: Exception) {
                        log.error(e) {
                            "Salesforce full load failed"
                        }
                    } finally {
                        salesforceFullLoadRunning.set(false)
                    }
                }

                Response(Status.ACCEPTED)
                    .body("Salesforce full load started")
            }
        }
    }

    val testSending5EnhetAnd5Underenhet: HttpHandler = {
        val today = LocalDate.now()

        val snapshot =
            PostgresDatabase.getEnhetsregisterSnapshot(today)

        if (
            snapshot == null ||
            snapshot.status != EnhetsregisterSnapshotStatus.READY
        ) {
            Response(Status.CONFLICT)
                .body("Today's snapshot is not READY")
        } else {
            val enheter =
                PostgresDatabase.fetchEnhetBatch(
                    snapshotDate = today,
                    afterOrgNumber = null,
                    limit = 5,
                )

            val underenheter =
                PostgresDatabase.fetchUnderenhetBatch(
                    snapshotDate = today,
                    afterOrgNumber = null,
                    limit = 5,
                )

            val changes =
                enheter.map { it.toNewChange() } +
                    underenheter.map { it.toNewChange() }

            salesforceClient.postChanges(changes)

            Response(OK)
                .body(
                    "Posted ${changes.size} organisations as a test" +
                        "(5 ENHET + 5 UNDERENHET)",
                )
        }
    }

    private fun runEnhetTodayDiff(
        today: LocalDate,
        yesterday: LocalDate,
    ): Pair<List<OrganisationChange>, ChangeStats> {
        val progress =
            PostgresDatabase.getSalesforceDiffProgress(
                today,
                OrgType.ENHET,
                SalesforceDiffPhase.TODAY,
            )

        var lastOrgNumber = progress?.lastOrgNumber
        var stats =
            ChangeStats(
                new = progress?.newCount ?: 0,
                updated = progress?.updatedCount ?: 0,
                removed = progress?.removedCount ?: 0,
            )

        val changes = mutableListOf<OrganisationChange>()

        PostgresDatabase.startSalesforceDiff(
            snapshotDate = today,
            orgType = OrgType.ENHET,
            phase = SalesforceDiffPhase.TODAY,
            existing = progress,
        )

        try {
            while (true) {
                val todayRows =
                    PostgresDatabase.fetchEnhetBatch(
                        snapshotDate = today,
                        afterOrgNumber = lastOrgNumber,
                        limit = DIFF_BATCH_SIZE,
                    )

                if (todayRows.isEmpty()) {
                    PostgresDatabase.markSalesforceDiffDone(
                        today,
                        OrgType.ENHET,
                        SalesforceDiffPhase.TODAY,
                    )

                    return changes to stats
                }

                val yesterdayRows =
                    PostgresDatabase.fetchEnhetRows(
                        snapshotDate = yesterday,
                        orgNumbers = todayRows.map { it.orgNumber },
                    )

                val yesterdayByOrg =
                    yesterdayRows.associateBy { it.orgNumber }

                val batchChanges =
                    todayRows.mapNotNull { todayRow ->
                        val yesterdayRow =
                            yesterdayByOrg[todayRow.orgNumber]

                        when {
                            yesterdayRow == null -> {
                                val change =
                                    OrganisationChange(
                                        orgNumber = todayRow.orgNumber,
                                        orgType = OrgType.ENHET,
                                        changeType = ChangeType.NEW,
                                        payloadHash = todayRow.payloadHash,
                                        payload = todayRow.payload,
                                    )

                                stats = stats.add(change)
                                change
                            }

                            yesterdayRow.payloadHash != todayRow.payloadHash -> {
                                val change =
                                    OrganisationChange(
                                        orgNumber = todayRow.orgNumber,
                                        orgType = OrgType.ENHET,
                                        changeType = ChangeType.UPDATED,
                                        payloadHash = todayRow.payloadHash,
                                        payload = todayRow.payload,
                                    )

                                stats = stats.add(change)
                                change
                            }

                            else -> null
                        }
                    }

                if (batchChanges.isNotEmpty()) {
                    changes += batchChanges

                    PostgresDatabase.saveSalesforceDiffOrganisations(
                        snapshotDate = today,
                        changes = batchChanges,
                    )
                }

                lastOrgNumber =
                    todayRows.last().orgNumber

                PostgresDatabase.updateSalesforceDiffProgress(
                    snapshotDate = today,
                    orgType = OrgType.ENHET,
                    phase = SalesforceDiffPhase.TODAY,
                    lastOrgNumber = lastOrgNumber,
                    newCount = stats.new,
                    updatedCount = stats.updated,
                    removedCount = stats.removed,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceDiffFailed(
                today,
                OrgType.ENHET,
                SalesforceDiffPhase.TODAY,
            )
            throw e
        }
    }

    private fun runUnderenhetTodayDiff(
        today: LocalDate,
        yesterday: LocalDate,
    ): Pair<List<OrganisationChange>, ChangeStats> {
        val progress =
            PostgresDatabase.getSalesforceDiffProgress(
                today,
                OrgType.UNDERENHET,
                SalesforceDiffPhase.TODAY,
            )

        var lastOrgNumber = progress?.lastOrgNumber
        var stats =
            ChangeStats(
                new = progress?.newCount ?: 0,
                updated = progress?.updatedCount ?: 0,
                removed = progress?.removedCount ?: 0,
            )

        val changes = mutableListOf<OrganisationChange>()

        PostgresDatabase.startSalesforceDiff(
            snapshotDate = today,
            orgType = OrgType.UNDERENHET,
            phase = SalesforceDiffPhase.TODAY,
            existing = progress,
        )

        try {
            while (true) {
                val todayRows =
                    PostgresDatabase.fetchUnderenhetBatch(
                        snapshotDate = today,
                        afterOrgNumber = lastOrgNumber,
                        limit = DIFF_BATCH_SIZE,
                    )

                if (todayRows.isEmpty()) {
                    PostgresDatabase.markSalesforceDiffDone(
                        today,
                        OrgType.UNDERENHET,
                        SalesforceDiffPhase.TODAY,
                    )

                    return changes to stats
                }

                val yesterdayRows =
                    PostgresDatabase.fetchUnderenhetRows(
                        snapshotDate = yesterday,
                        orgNumbers = todayRows.map { it.orgNumber },
                    )

                val yesterdayByOrg =
                    yesterdayRows.associateBy { it.orgNumber }

                val batchChanges =
                    todayRows.mapNotNull { todayRow ->
                        val yesterdayRow =
                            yesterdayByOrg[todayRow.orgNumber]

                        when {
                            yesterdayRow == null -> {
                                val change =
                                    OrganisationChange(
                                        orgNumber = todayRow.orgNumber,
                                        orgType = OrgType.UNDERENHET,
                                        changeType = ChangeType.NEW,
                                        payloadHash = todayRow.payloadHash,
                                        payload = todayRow.payload,
                                    )

                                stats = stats.add(change)
                                change
                            }

                            yesterdayRow.payloadHash != todayRow.payloadHash -> {
                                val change =
                                    OrganisationChange(
                                        orgNumber = todayRow.orgNumber,
                                        orgType = OrgType.UNDERENHET,
                                        changeType = ChangeType.UPDATED,
                                        payloadHash = todayRow.payloadHash,
                                        payload = todayRow.payload,
                                    )

                                stats = stats.add(change)
                                change
                            }

                            else -> null
                        }
                    }

                if (batchChanges.isNotEmpty()) {
                    changes += batchChanges

                    PostgresDatabase.saveSalesforceDiffOrganisations(
                        snapshotDate = today,
                        changes = batchChanges,
                    )
                }

                lastOrgNumber =
                    todayRows.last().orgNumber

                PostgresDatabase.updateSalesforceDiffProgress(
                    snapshotDate = today,
                    orgType = OrgType.UNDERENHET,
                    phase = SalesforceDiffPhase.TODAY,
                    lastOrgNumber = lastOrgNumber,
                    newCount = stats.new,
                    updatedCount = stats.updated,
                    removedCount = stats.removed,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceDiffFailed(
                today,
                OrgType.UNDERENHET,
                SalesforceDiffPhase.TODAY,
            )
            throw e
        }
    }

    private fun runEnhetRemovedDiff(
        today: LocalDate,
        yesterday: LocalDate,
    ): Pair<List<OrganisationChange>, ChangeStats> {
        val progress =
            PostgresDatabase.getSalesforceDiffProgress(
                today,
                OrgType.ENHET,
                SalesforceDiffPhase.REMOVED,
            )

        var lastOrgNumber = progress?.lastOrgNumber
        var stats =
            ChangeStats(
                new = progress?.newCount ?: 0,
                updated = progress?.updatedCount ?: 0,
                removed = progress?.removedCount ?: 0,
            )

        val changes = mutableListOf<OrganisationChange>()

        PostgresDatabase.startSalesforceDiff(
            snapshotDate = today,
            orgType = OrgType.ENHET,
            phase = SalesforceDiffPhase.REMOVED,
            existing = progress,
        )

        try {
            while (true) {
                val yesterdayRows =
                    PostgresDatabase.fetchEnhetBatch(
                        snapshotDate = yesterday,
                        afterOrgNumber = lastOrgNumber,
                        limit = DIFF_BATCH_SIZE,
                    )

                if (yesterdayRows.isEmpty()) {
                    PostgresDatabase.markSalesforceDiffDone(
                        today,
                        OrgType.ENHET,
                        SalesforceDiffPhase.REMOVED,
                    )

                    return changes to stats
                }

                val todayRows =
                    PostgresDatabase.fetchEnhetRows(
                        snapshotDate = today,
                        orgNumbers = yesterdayRows.map { it.orgNumber },
                    )

                val todayOrgNumbers =
                    todayRows
                        .asSequence()
                        .map { it.orgNumber }
                        .toSet()

                val batchChanges =
                    yesterdayRows
                        .filter {
                            it.orgNumber !in todayOrgNumbers
                        }.map {
                            OrganisationChange(
                                orgNumber = it.orgNumber,
                                orgType = OrgType.ENHET,
                                changeType = ChangeType.REMOVED,
                                payloadHash = null,
                                payload = null,
                            )
                        }

                batchChanges.forEach {
                    stats = stats.add(it)
                }

                if (batchChanges.isNotEmpty()) {
                    changes += batchChanges

                    PostgresDatabase.saveSalesforceDiffOrganisations(
                        snapshotDate = today,
                        changes = batchChanges,
                    )
                }

                lastOrgNumber =
                    yesterdayRows.last().orgNumber

                PostgresDatabase.updateSalesforceDiffProgress(
                    snapshotDate = today,
                    orgType = OrgType.ENHET,
                    phase = SalesforceDiffPhase.REMOVED,
                    lastOrgNumber = lastOrgNumber,
                    newCount = stats.new,
                    updatedCount = stats.updated,
                    removedCount = stats.removed,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceDiffFailed(
                today,
                OrgType.ENHET,
                SalesforceDiffPhase.REMOVED,
            )
            throw e
        }
    }

    private fun runUnderenhetRemovedDiff(
        today: LocalDate,
        yesterday: LocalDate,
    ): Pair<List<OrganisationChange>, ChangeStats> {
        val progress =
            PostgresDatabase.getSalesforceDiffProgress(
                today,
                OrgType.UNDERENHET,
                SalesforceDiffPhase.REMOVED,
            )

        var lastOrgNumber = progress?.lastOrgNumber
        var stats =
            ChangeStats(
                new = progress?.newCount ?: 0,
                updated = progress?.updatedCount ?: 0,
                removed = progress?.removedCount ?: 0,
            )

        val changes = mutableListOf<OrganisationChange>()

        PostgresDatabase.startSalesforceDiff(
            snapshotDate = today,
            orgType = OrgType.UNDERENHET,
            phase = SalesforceDiffPhase.REMOVED,
            existing = progress,
        )

        try {
            while (true) {
                val yesterdayRows =
                    PostgresDatabase.fetchUnderenhetBatch(
                        snapshotDate = yesterday,
                        afterOrgNumber = lastOrgNumber,
                        limit = DIFF_BATCH_SIZE,
                    )

                if (yesterdayRows.isEmpty()) {
                    PostgresDatabase.markSalesforceDiffDone(
                        today,
                        OrgType.UNDERENHET,
                        SalesforceDiffPhase.REMOVED,
                    )

                    return changes to stats
                }

                val todayRows =
                    PostgresDatabase.fetchUnderenhetRows(
                        snapshotDate = today,
                        orgNumbers = yesterdayRows.map { it.orgNumber },
                    )

                val todayOrgNumbers =
                    todayRows
                        .asSequence()
                        .map { it.orgNumber }
                        .toSet()

                val batchChanges =
                    yesterdayRows
                        .filter {
                            it.orgNumber !in todayOrgNumbers
                        }.map {
                            OrganisationChange(
                                orgNumber = it.orgNumber,
                                orgType = OrgType.UNDERENHET,
                                changeType = ChangeType.REMOVED,
                                payloadHash = null,
                                payload = null,
                            )
                        }

                batchChanges.forEach {
                    stats = stats.add(it)
                }

                if (batchChanges.isNotEmpty()) {
                    changes += batchChanges

                    PostgresDatabase.saveSalesforceDiffOrganisations(
                        snapshotDate = today,
                        changes = batchChanges,
                    )
                }

                lastOrgNumber =
                    yesterdayRows.last().orgNumber

                PostgresDatabase.updateSalesforceDiffProgress(
                    snapshotDate = today,
                    orgType = OrgType.UNDERENHET,
                    phase = SalesforceDiffPhase.REMOVED,
                    lastOrgNumber = lastOrgNumber,
                    newCount = stats.new,
                    updatedCount = stats.updated,
                    removedCount = stats.removed,
                )
            }
        } catch (e: Exception) {
            PostgresDatabase.markSalesforceDiffFailed(
                today,
                OrgType.UNDERENHET,
                SalesforceDiffPhase.REMOVED,
            )
            throw e
        }
    }

    private fun runSalesforceDiff(
        today: LocalDate,
        yesterday: LocalDate,
    ): DiffResult {
        val (enhetTodayChanges, enhetTodayStats) =
            runEnhetTodayDiff(today, yesterday)

        val (enhetRemovedChanges, enhetRemovedStats) =
            runEnhetRemovedDiff(today, yesterday)

        val (underenhetTodayChanges, underenhetTodayStats) =
            runUnderenhetTodayDiff(today, yesterday)

        val (underenhetRemovedChanges, underenhetRemovedStats) =
            runUnderenhetRemovedDiff(today, yesterday)

        val changes =
            enhetTodayChanges +
                enhetRemovedChanges +
                underenhetTodayChanges +
                underenhetRemovedChanges

        return DiffResult(
            changes = changes,
            enhet =
                enhetTodayStats +
                    enhetRemovedStats,
            underenhet =
                underenhetTodayStats +
                    underenhetRemovedStats,
        )
    }

    private fun diffProgress(date: LocalDate): Map<String, Any?> {
        val result =
            OrgType.entries.associate { orgType ->
                val today =
                    PostgresDatabase.getSalesforceDiffProgress(
                        date,
                        orgType,
                        SalesforceDiffPhase.TODAY,
                    )

                val removed =
                    PostgresDatabase.getSalesforceDiffProgress(
                        date,
                        orgType,
                        SalesforceDiffPhase.REMOVED,
                    )

                val status =
                    when {
                        today?.status == SalesforceDiffStatus.FAILED ||
                            removed?.status == SalesforceDiffStatus.FAILED ->
                            SalesforceDiffStatus.FAILED

                        today?.status == SalesforceDiffStatus.IN_PROGRESS ||
                            removed?.status == SalesforceDiffStatus.IN_PROGRESS ->
                            SalesforceDiffStatus.IN_PROGRESS

                        today?.status == SalesforceDiffStatus.DONE &&
                            removed?.status == SalesforceDiffStatus.DONE ->
                            SalesforceDiffStatus.DONE

                        else ->
                            SalesforceDiffStatus.NOT_STARTED
                    }

                val lastOrgNumber =
                    when {
                        removed?.status == SalesforceDiffStatus.IN_PROGRESS -> removed.lastOrgNumber
                        today?.status == SalesforceDiffStatus.IN_PROGRESS -> today.lastOrgNumber
                        removed?.status == SalesforceDiffStatus.DONE -> removed.lastOrgNumber
                        else -> today?.lastOrgNumber
                    }

                orgType.name.lowercase() to
                    mapOf(
                        "status" to status.name,
                        "lastOrgNumber" to lastOrgNumber,
                        "new" to (today?.newCount ?: 0),
                        "updated" to (today?.updatedCount ?: 0),
                        "removed" to (removed?.removedCount ?: 0),
                    )
            }

        val statuses =
            OrgType.entries.mapNotNull { orgType ->
                @Suppress("UNCHECKED_CAST")
                (result[orgType.name.lowercase()] as? Map<String, Any?>)
                    ?.get("status")
                    ?.toString()
            }

        val globalStatus =
            when {
                statuses.any { it == SalesforceDiffStatus.FAILED.name } ->
                    SalesforceDiffStatus.FAILED.name

                statuses.any { it == SalesforceDiffStatus.IN_PROGRESS.name } ->
                    SalesforceDiffStatus.IN_PROGRESS.name

                statuses.size == OrgType.entries.size &&
                    statuses.all { it == SalesforceDiffStatus.DONE.name } ->
                    SalesforceDiffStatus.DONE.name

                else ->
                    SalesforceDiffStatus.NOT_STARTED.name
            }

        return mapOf(
            "status" to globalStatus,
            "enhet" to result["enhet"],
            "underenhet" to result["underenhet"],
        )
    }

    private fun hasSalesforceDiffInProgress(date: LocalDate): Boolean =
        OrgType.entries.any { orgType ->
            SalesforceDiffPhase.entries.any { phase ->
                PostgresDatabase
                    .getSalesforceDiffProgress(
                        date,
                        orgType,
                        phase,
                    )?.status == SalesforceDiffStatus.IN_PROGRESS
            }
        }

    private fun hasSalesforceInitialLoadInProgress(date: LocalDate): Boolean =
        OrgType.entries.any { orgType ->
            PostgresDatabase
                .getSalesforceInitialLoadProgress(
                    date,
                    orgType,
                )?.status == SalesforceInitialLoadStatus.IN_PROGRESS
        }

    private fun recoverAfterStartup() {
        runExecutor.submit {
            try {
                // Give Hikari/DB and the rest of the application a moment to settle.
                Thread.sleep(2_000)

                val today = LocalDate.now()

                synchronized(runLock) {
                    val snapshot =
                        PostgresDatabase.getEnhetsregisterSnapshot(today)

                    when (snapshot?.status) {
                        EnhetsregisterSnapshotStatus.LOADING -> {
                            log.warn {
                                "Found interrupted Enhetsregister snapshot for $today. " +
                                    "Removing partial snapshot and restarting."
                            }

                            PostgresDatabase.resetSnapshot(today)

                            startTodayRun(today)
                        }

                        EnhetsregisterSnapshotStatus.READY -> {
                            when {
                                hasSalesforceDiffInProgress(today) -> {
                                    log.info {
                                        "Found interrupted Salesforce diff for $today. " +
                                            "Resuming."
                                    }

                                    startTodayRun(today)
                                }

                                hasSalesforceInitialLoadInProgress(today) -> {
                                    log.info {
                                        "Found interrupted Salesforce initial load for $today."
                                    }

                                    OrgType.entries.forEach { orgType ->
                                        val progress =
                                            PostgresDatabase
                                                .getSalesforceInitialLoadProgress(
                                                    snapshotDate = today,
                                                    orgType = orgType,
                                                )

                                        if (
                                            progress?.status ==
                                            SalesforceInitialLoadStatus.IN_PROGRESS
                                        ) {
                                            log.info {
                                                "Resuming Salesforce initial load " +
                                                    "for $orgType from " +
                                                    "${progress.lastOrgNumber}"
                                            }

                                            startSalesforceInitialLoad(
                                                snapshotDate = today,
                                                orgType = orgType,
                                                lastOrgNumber = progress.lastOrgNumber,
                                            )
                                        }
                                    }
                                }

                                else -> {
                                    log.info {
                                        "No interrupted operation found for $today"
                                    }
                                }
                            }
                        }

                        EnhetsregisterSnapshotStatus.FAILED -> {
                            log.info {
                                "Today's snapshot is FAILED. " +
                                    "Waiting for normal triggerRun handling."
                            }
                        }

                        null -> {
                            log.info {
                                "No snapshot found for $today. Nothing to recover."
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                log.error(e) {
                    "Startup recovery failed"
                }
            }
        }
    }

    private val databaseDiagnosticsHandler: HttpHandler = {
        try {
            val diagnostics =
                PostgresDatabase.databaseDiagnostics()

            Response(Status.OK)
                .header(
                    "Content-Type",
                    "application/json",
                ).body(
                    gson.toJson(diagnostics),
                )
        } catch (e: Exception) {
            log.error(e) {
                "Could not read database diagnostics"
            }

            Response(Status.INTERNAL_SERVER_ERROR)
                .body(
                    "Could not read database diagnostics: " +
                        e.message,
                )
        }
    }
}

enum class ChangeType {
    NEW,
    UPDATED,
    REMOVED,
}

enum class OrgType {
    ENHET,
    UNDERENHET,
}

data class OrganisationChange(
    val orgNumber: String,
    val orgType: OrgType,
    val changeType: ChangeType,
    val payloadHash: String?,
    val payload: String?,
)

fun OrganisationChange.toSalesforceMessage(): KafkaMessage =
    when (changeType) {
        ChangeType.NEW,
        ChangeType.UPDATED,
        -> {
            val payload =
                requireNotNull(payload) {
                    "Payload is required for $changeType"
                }

            val payloadHash =
                requireNotNull(payloadHash) {
                    "Payload hash is required for $changeType"
                }

            KafkaMessage(
                CRM_Topic__c = orgTopic,
                CRM_Key__c =
                    "$orgNumber#${orgType.name}#$payloadHash",
                CRM_Value__c = payload.encodeB64(),
            )
        }

        ChangeType.REMOVED -> {
            KafkaMessage(
                CRM_Topic__c = tombstoneTopic,
                CRM_Key__c = orgNumber,
                CRM_Value__c = orgNumber,
            )
        }
    }

fun EnhetSnapshot.toNewChange() =
    OrganisationChange(
        orgNumber = orgNumber,
        orgType = OrgType.ENHET,
        changeType = ChangeType.NEW,
        payloadHash = payloadHash,
        payload = payload,
    )

fun UnderenhetSnapshot.toNewChange() =
    OrganisationChange(
        orgNumber = orgNumber,
        orgType = OrgType.UNDERENHET,
        changeType = ChangeType.NEW,
        payloadHash = payloadHash,
        payload = payload,
    )

data class ChangeStats(
    val new: Int = 0,
    val updated: Int = 0,
    val removed: Int = 0,
) {
    operator fun plus(other: ChangeStats) =
        ChangeStats(
            new = new + other.new,
            updated = updated + other.updated,
            removed = removed + other.removed,
        )

    fun add(change: OrganisationChange) =
        when (change.changeType) {
            ChangeType.NEW ->
                copy(new = new + 1)

            ChangeType.UPDATED ->
                copy(updated = updated + 1)

            ChangeType.REMOVED ->
                copy(removed = removed + 1)
        }
}

data class DiffResult(
    val changes: List<OrganisationChange>,
    val enhet: ChangeStats,
    val underenhet: ChangeStats,
) {
    val total: Int
        get() = changes.size
}

data class OrganisationSnapshotResponse(
    val orgNumber: String,
    val snapshots: List<OrganisationSnapshot>,
)

data class OrganisationSnapshot(
    val orgType: String,
    val name: String?,
    val registrationDate: String?,
    val payload: String,
)

data class DiffOrganisationRow(
    val orgNumber: String,
    val orgType: String,
    val changeType: String,
)

data class DatabaseTableDiagnostic(
    val table: String,
    val totalSize: String,
    val totalSizeBytes: Long,
    val rowEstimate: Long,
)

data class SnapshotDateDiagnostic(
    val enhet: List<LocalDate>,
    val underenhet: List<LocalDate>,
    val metadata: List<LocalDate>,
)

data class DatabaseDiagnostic(
    val tables: List<DatabaseTableDiagnostic>,
    val snapshotDates: SnapshotDateDiagnostic,
)
