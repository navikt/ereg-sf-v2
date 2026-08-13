package no.nav.ereg

import EnhetSnapshot
import UnderenhetSnapshot
import com.google.gson.Gson
import com.google.gson.JsonParser
import com.google.gson.stream.JsonReader
import mu.KotlinLogging
import no.nav.ereg.token.AuthRouteBuilder
import no.nav.ereg.token.DefaultTokenValidator
import no.nav.ereg.token.MockTokenValidator
import no.nav.sf.keytool.db.PostgresDatabase
import okhttp3.OkHttpClient
import org.http4k.client.OkHttp
import org.http4k.core.BodyMode
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.core.Status.Companion.OK
import org.http4k.filter.gunzippedStream
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Http4kServer
import org.http4k.server.Netty
import org.http4k.server.asServer
import java.io.InputStream
import java.io.InputStreamReader
import java.security.MessageDigest
import java.time.LocalDate
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

const val DB_BATCH_SIZE = 2_000
const val ENHETER_URL =
    "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"

const val ENHET_ACCEPT_HEADER =
    "application/vnd.brreg.enhetsregisteret.enhet.v2+gzip;charset=UTF-8"

const val UNDERENHETER_URL =
    "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned"

const val UNDERENHET_ACCEPT_HEADER =
    "application/vnd.brreg.enhetsregisteret.underenhet.v2+gzip;charset=UTF-8"

class Application {
    private val log = KotlinLogging.logger { }

    val gson = Gson()

    val local: Boolean = System.getenv(env_NAIS_CLUSTER_NAME) == null

    val tokenValidator = if (local) MockTokenValidator() else DefaultTokenValidator()

    val cluster = if (local) "local" else env(env_NAIS_CLUSTER_NAME)

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
        routes(
            "/internal/isAlive" bind Method.GET to { Response(OK) },
            "/internal/isReady" bind Method.GET to { Response(OK) },
            "/internal/metrics" bind Method.GET to Metrics.metricsHttpHandler,
            "/internal/hello" bind Method.GET to { Response(OK).body("Hello") },
            "/internal/secrethello" authbind Method.GET to { Response(OK).body("Secret Hello") },
            "/internal/clearDb" bind Method.GET to clearDbHandler,
            "/internal/initDb" bind Method.GET to initDbHandler,
            "/internal/triggerRun" bind Method.GET to triggerRunHandler,
            "/internal/status" bind Method.GET to {
                runResponse(LocalDate.now())
            },
            "/internal/statusYesterday" bind Method.GET to {
                runResponse(LocalDate.now().minusDays(1))
            },
        )

    /**
     * authbind: a variant of bind that takes care of authentication with use of tokenValidator
     */
    infix fun String.authbind(method: Method) = AuthRouteBuilder(this, method, tokenValidator)

    fun start() {
        log.info { "Starting in cluster $cluster" }
        apiServer(8080).start()
    }

    private val clearDbHandler: HttpHandler = {
        PostgresDatabase.createEnhetsregisterSnapshotTable(true)
        PostgresDatabase.createEnhetSnapshotTable(true)
        PostgresDatabase.createUnderenhetSnapshotTable(true)
        Response(OK).body("Tables recreated")
    }

    private val initDbHandler: HttpHandler = {
        PostgresDatabase.createEnhetsregisterSnapshotTable(false)
        PostgresDatabase.createEnhetSnapshotTable(false)
        PostgresDatabase.createUnderenhetSnapshotTable(false)
        Response(OK).body("Tables created")
    }

    private fun importEnheter(
        input: InputStream,
        snapshotDate: LocalDate,
    ) {
        val batch = ArrayList<EnhetSnapshot>(DB_BATCH_SIZE)
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

                if (batch.size >= DB_BATCH_SIZE) {
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
            ArrayList<UnderenhetSnapshot>(DB_BATCH_SIZE)

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

                if (batch.size >= DB_BATCH_SIZE) {
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

    fun yesterdayRun(): Run = fetchRun(LocalDate.now().minusDays(1))

    fun todayRun(): Run = fetchRun(LocalDate.now())

    fun runResponse(date: LocalDate): Response {
        val run = fetchRun(date)

        return Response(OK)
            .header("Content-Type", "application/json")
            .body(
                gson.toJson(
                    mapOf(
                        "date" to date.toString(),
                        "status" to run.status.name,
                        "enhetCount" to run.enhetCount,
                        "underenhetCount" to run.underenhetCount,
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
                        .body(
                            "Today's run is already in progress",
                        )
                }

                EnhetsregisterSnapshotStatus.READY -> {
                    Response(Status.OK)
                        .body(
                            "Today's run has already completed successfully",
                        )
                }

                EnhetsregisterSnapshotStatus.FAILED -> {

                    PostgresDatabase.deleteEnhetsregisterSnapshot(today)

                    PostgresDatabase.createEnhetsregisterSnapshot(today)

                    startTodayRun(today)

                    Response(Status.ACCEPTED)
                        .body("Today's run restarted")
                }

                null -> {

                    PostgresDatabase.createEnhetsregisterSnapshot(today)

                    startTodayRun(today)

                    Response(Status.ACCEPTED)
                        .body("Run started")
                }
            }
        }
    }

    private fun startTodayRun(snapshotDate: LocalDate) {
        runExecutor.submit {
            try {
                log.info {
                    "Starting Enhetsregister ENHET snapshot " +
                        "for $snapshotDate"
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
                    underenhetCount = 0,
                    sourceChecksum = null,
                )

                log.info {
                    "Enhetsregister ENHET snapshot completed " +
                        "for $snapshotDate: " +
                        "${snapshot.enhetCount ?: 0} events"
                }
            } catch (e: Exception) {
                log.error(e) {
                    "Enhetsregister snapshot failed " +
                        "for $snapshotDate"
                }

                PostgresDatabase.markSnapshotFailed(
                    snapshotDate,
                )
            }
        }
    }
}
