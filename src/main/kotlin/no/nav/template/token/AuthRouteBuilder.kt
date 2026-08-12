package no.nav.template.token

import no.nav.template.Metrics
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.PathMethod
import org.http4k.routing.RoutingHttpHandler

data class AuthRouteBuilder(
    val path: String,
    val method: Method,
    private val tokenValidator: TokenValidator,
) {
    infix fun to(action: HttpHandler): RoutingHttpHandler =
        PathMethod(path, method) to { request ->
            Metrics.apiCalls.labels(path).inc()
            val token = tokenValidator.firstValidToken(request)
            if (token != null) {
                action(request)
            } else {
                Response(Status.UNAUTHORIZED)
            }
        }
}
