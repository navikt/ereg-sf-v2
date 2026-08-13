package no.nav.ereg.salesforce

import mu.KotlinLogging
import no.nav.ereg.config_SALESFORCE_API_VERSION
import no.nav.ereg.env
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import java.io.File

private val log = KotlinLogging.logger {}

/**
 * Adapter code for merge old client with new access token handler. TODO Rewrite to more standard practice (two separate aoos not needed)
 */
class SalesforceClient(
    private val httpClient: HttpHandler = OkHttp(),
    private val accessTokenHandler: DefaultAccessTokenHandler = DefaultAccessTokenHandler(),
) {
    private fun post(body: String): Response {
        val dstUrl =
            "${accessTokenHandler.instanceUrl}/services/data/${env(config_SALESFORCE_API_VERSION)}/composite/sobjects"

        val headers: Headers =
            listOf(
                "Authorization" to "Bearer ${accessTokenHandler.accessToken}",
                "Content-Type" to "application/json;charset=UTF-8",
            )

        val request =
            Request(Method.POST, dstUrl)
                .headers(headers)
                .body(body)

        File("/tmp/files/latestPostRequest").writeText(request.toMessage())

        return httpClient(request)
    }

    fun postRecords(kafkaMessages: List<KafkaMessage>): Response {
        val requestBody = SFsObjectRest(records = kafkaMessages).toJson()
        return post(requestBody)
    }
}
