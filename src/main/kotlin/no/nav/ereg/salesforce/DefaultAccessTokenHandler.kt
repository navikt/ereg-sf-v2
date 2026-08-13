package no.nav.ereg.salesforce

import com.google.gson.Gson
import mu.KotlinLogging
import no.nav.ereg.config_SF_JWT_USERNAME
import no.nav.ereg.config_SF_TOKEN_HOST
import no.nav.ereg.env
import no.nav.ereg.secret_SF_JWT_CLIENT_ID
import no.nav.ereg.secret_SF_JWT_KEYSTORE_B64
import no.nav.ereg.secret_SF_JWT_KEYSTORE_PASSWORD
import org.http4k.client.OkHttp
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status
import org.http4k.core.body.toBody
import java.security.KeyStore
import java.security.PrivateKey
import java.security.Signature
import java.util.Base64

/**
 * A handler for oauth2 access flow to salesforce.
 * @see [sf.remoteaccess_oauth_jwt_flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_jwt_flow.htm&type=5)
 *
 * Fetches and caches access token, also retrieves instance url
 *
 * Key alias set to "jwt". Assumption for certificates created through keytool, see https://github.com/navikt/sf-keytool
 */
class DefaultAccessTokenHandler(
    private val sfTokenHost: String = env(config_SF_TOKEN_HOST),
    private val sfUsername: String = env(config_SF_JWT_USERNAME),
    private val sfClientId: String = env(secret_SF_JWT_CLIENT_ID),
    private val keystoreJksB64: String = env(secret_SF_JWT_KEYSTORE_B64),
    private val keystorePassword: String = env(secret_SF_JWT_KEYSTORE_PASSWORD),
    private val alias: String = "jwt",
) : AccessTokenHandler {
    override val accessToken get() = fetch().first
    override val instanceUrl get() = fetch().second
    override val tenantId get() = fetch().third

    private val log = KotlinLogging.logger {}
    private val client: HttpHandler = OkHttp()
    private val gson = Gson()

    private val expTimeSecondsClaim = 3600
    private var cached: Triple<String, String, String>? = null
    private var expireAtMillis = 0L

    fun testAccess(): Boolean = fetch(true).first != ""

    private fun fetch(ignoreCache: Boolean = false): Triple<String, String, String> {
        val now = System.currentTimeMillis()
        if (!ignoreCache && cached != null && now < expireAtMillis) {
            log.debug { "Using cached Salesforce access token" }
            return cached!!
        }

        val jwt = buildJwtAssertion()
        val request =
            Request(Method.POST, sfTokenHost)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(
                    listOf(
                        "grant_type" to "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion" to jwt,
                    ).toBody(),
                )

        val response = client(request)
        if (response.status != Status.OK) {
            throw IllegalStateException(
                "Salesforce token request failed: ${response.status}\n${response.bodyString()}",
            )
        }

        val parsed =
            gson.fromJson(response.bodyString(), AccessTokenResponse::class.java)

        val triplet =
            Triple(
                parsed.access_token,
                parsed.instance_url,
                parsed.id.split("/")[4],
            )

        cached = triplet
        expireAtMillis = now + 10 * 60 * 1000 // cache 10 minutes

        return triplet
    }

    private fun buildJwtAssertion(): String {
        val exp =
            (System.currentTimeMillis() / 1000) + expTimeSecondsClaim

        val claim =
            JWTClaim(
                iss = sfClientId,
                aud = sfTokenHost,
                sub = sfUsername,
                exp = exp,
            )

        val headerJson = gson.toJson(JWTHeader("RS256"))
        val claimJson = gson.toJson(claim)

        val unsigned =
            "${headerJson.encodeB64UrlSafe()}.${claimJson.encodeB64UrlSafe()}"

        val privateKey = loadPrivateKey()
        val signature = privateKey.sign(unsigned.toByteArray())

        return "$unsigned.$signature"
    }

    private fun loadPrivateKey(): PrivateKey =
        KeyStore
            .getInstance("JKS")
            .apply {
                load(
                    keystoreJksB64.decodeB64().inputStream(),
                    keystorePassword.toCharArray(),
                )
            }.getKey(alias, keystorePassword.toCharArray()) as PrivateKey

    private fun PrivateKey.sign(data: ByteArray): String =
        Signature
            .getInstance("SHA256withRSA")
            .apply {
                initSign(this@sign)
                update(data)
            }.sign()
            .encodeB64UrlSafe()

    private fun ByteArray.encodeB64UrlSafe(): String = Base64.getUrlEncoder().withoutPadding().encodeToString(this)

    private fun String.decodeB64(): ByteArray = Base64.getMimeDecoder().decode(this)

    private fun String.encodeB64UrlSafe(): String = toByteArray(Charsets.UTF_8).encodeB64UrlSafe()

    private data class JWTClaim(
        val iss: String,
        val aud: String,
        val sub: String,
        val exp: Long,
    )

    private data class JWTHeader(
        val alg: String,
    )

    private data class AccessTokenResponse(
        val access_token: String,
        val instance_url: String,
        val id: String,
        val token_type: String,
        val scope: String,
    )
}
