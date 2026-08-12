package no.nav.ereg

import mu.KotlinLogging
import no.nav.ereg.token.AuthRouteBuilder
import no.nav.ereg.token.DefaultTokenValidator
import no.nav.ereg.token.MockTokenValidator
import no.nav.sf.keytool.db.PostgresDatabase
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status.Companion.OK
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Http4kServer
import org.http4k.server.Netty
import org.http4k.server.asServer

class Application {
    private val log = KotlinLogging.logger { }

    val local: Boolean = System.getenv(env_NAIS_CLUSTER_NAME) == null

    val tokenValidator = if (local) MockTokenValidator() else DefaultTokenValidator()

    val cluster = if (local) "local" else env(env_NAIS_CLUSTER_NAME)

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
}
