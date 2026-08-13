package no.nav.ereg.salesforce

import com.google.gson.reflect.TypeToken
import mu.KotlinLogging
import no.nav.ereg.Metrics
import no.nav.ereg.OrganisationChange
import no.nav.ereg.config_SALESFORCE_API_VERSION
import no.nav.ereg.env
import no.nav.ereg.toSalesforceMessage
import org.http4k.client.OkHttp
import org.http4k.core.Headers
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import java.lang.reflect.Type

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

    fun postChanges(changes: List<OrganisationChange>) {
        changes
            .chunked(100)
            .forEachIndexed { index, batch ->

                val requestBody =
                    SFsObjectRest(
                        records =
                            batch.map(
                                OrganisationChange::toSalesforceMessage,
                            ),
                    ).toJson()

                val response = post(requestBody)

                if (!isSuccess(response)) {
                    error(
                        "Salesforce batch $index failed " +
                            "(${batch.size} records)",
                    )
                }

                batch.forEach { change ->
                    Metrics.publishedOrgs
                        .labels(
                            change.orgType.name,
                            change.changeType.name,
                        ).inc()
                }
            }
    }

    private fun isSuccess(response: Response): Boolean {
        if (response.status != Status.OK) {
            log.error {
                "Post request to Salesforce failed - " +
                    "${response.status.description}(${response.status.code})"
            }
            return false
        }

        return try {
            val statusType: Type =
                object : TypeToken<List<SFsObjectStatus>>() {}.type

            val results =
                gson.fromJson<List<SFsObjectStatus>>(
                    response.bodyString(),
                    statusType,
                )

            when {
                results.isEmpty() -> {
                    log.error {
                        "Salesforce response contains no status objects"
                    }
                    false
                }

                results.all { it.success } -> true

                else -> {
                    results
                        .filterNot { it.success }
                        .flatMap { it.errors }
                        .forEach { error ->
                            log.error {
                                "Salesforce record failed: " +
                                    "statusCode=${error.statusCode}, " +
                                    "message=${error.message}, " +
                                    "fields=${error.fields}"
                            }
                        }

                    false
                }
            }
        } catch (e: Exception) {
            log.error(e) {
                "Could not parse Salesforce response"
            }
            false
        }
    }
}
