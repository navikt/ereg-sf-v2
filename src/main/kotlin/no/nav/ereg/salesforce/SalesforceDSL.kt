package no.nav.ereg.salesforce

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.prometheus.client.Histogram
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.http4k.client.OkHttp
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import java.lang.reflect.Type
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.URI
import java.security.KeyStore
import java.security.PrivateKey

private val log = KotlinLogging.logger { }

val gson = Gson()

fun OkHttp.supportProxy(httpsProxy: String): HttpHandler =
    httpsProxy.let { p ->
        when {
            p.isEmpty() -> this()
            else -> {
                val uri = URI(p)
                val proxy =
                    Proxy(
                        Proxy.Type.HTTP,
                        InetSocketAddress(uri.host, uri.port),
                    )
                this(
                    client =
                        okhttp3.OkHttpClient
                            .Builder()
                            .proxy(proxy)
                            .build(),
                )
            }
        }
    }

fun HttpHandler.measure(
    r: Request,
    m: Histogram,
): Response =
    m.startTimer().let { rt ->
        this(r).also {
            rt.observeDuration() // Histogram will store response time
            File("/tmp/files/lastTokenCall").writeText("uri: ${r.uri}, method: ${r.method}, body: ${r.body}, headers ${r.headers}")
        }
    }

fun ByteArray.encodeB64(): String =
    String(
        java.util.Base64
            .getUrlEncoder()
            .withoutPadding()
            .encode(this),
    )

fun String.encodeB64UrlSafe(): String = this.toByteArray(Charsets.UTF_8).encodeB64()

fun String.encodeB64(): String = this.toByteArray(Charsets.UTF_8).encodeB64()

fun String.decodeB64(): ByteArray =
    java.util.Base64
        .getMimeDecoder()
        .decode(this)

/**
 * Getting access token from Salesforce is a little bit involving due to
 * - Need a private key from a key store where the public key is in the connected app in Salesforce
 * - Need to sign a claim (some facts about salesforce) with the private key
 * - Need an access token request using the signed claim
 */
sealed class KeystoreBase {
    object Missing : KeystoreBase()

    data class Exists(
        val privateKey: PrivateKey,
    ) : KeystoreBase() {
        fun sign(data: ByteArray): SignatureBase =
            runCatching {
                java.security.Signature
                    .getInstance("SHA256withRSA")
                    .apply {
                        initSign(privateKey)
                        update(data)
                    }.run { SignatureBase.Exists(sign().encodeB64()) }
            }.onFailure { log.error { "Signing failed - ${it.localizedMessage}" } }
                .getOrDefault(SignatureBase.Missing)
    }

    fun signCheckIsOk(): Boolean =
        when (this) {
            is Missing -> false
            else -> ((this as Exists).sign("something".toByteArray())) != SignatureBase.Missing
        }

    companion object {
        fun fromBase64(
            ksB64: String,
            ksPwd: String,
            pkAlias: String,
            pkPwd: String,
        ): KeystoreBase =
            runCatching {
                Exists(
                    KeyStore
                        .getInstance("JKS")
                        .apply { load(ksB64.decodeB64().inputStream(), ksPwd.toCharArray()) }
                        .run { getKey(pkAlias, pkPwd.toCharArray()) as PrivateKey },
                )
            }.onFailure {
                log.error { "Keystore issues - ${it.localizedMessage}" }
            }.getOrDefault(Missing)
    }
}

sealed class SignatureBase {
    object Missing : SignatureBase()

    data class Exists(
        val content: String,
    ) : SignatureBase()
}

sealed class JWTClaimBase {
    object Missing : JWTClaimBase()

    data class Exists(
        val iss: String,
        val aud: String,
        val sub: String,
        val exp: String,
    ) : JWTClaimBase() {
        private fun toJson(): String = gson.toJson(this)

        fun addHeader(): String = "${Header().toJson().encodeB64UrlSafe()}.${this.toJson().encodeB64UrlSafe()}"
    }

    companion object {
        fun fromJson(data: String): JWTClaimBase =
            runCatching {
                gson.fromJson(data, Exists::class.java)
            }.onFailure {
                log.error { "Parsing of JWTClaim failed" }
            }.getOrDefault(Missing)
    }

    // @Serializable
    data class Header(
        val alg: String = "RS256",
    ) {
        fun toJson(): String = gson.toJson(this)
    }
}

sealed class SFAccessToken {
    object Missing : SFAccessToken()

    // @Serializable
    data class Exists(
        val access_token: String = "",
        val scope: String = "",
        val instance_url: String = "",
        val id: String = "",
        val token_type: String = "",
        val issued_at: String = "",
        val signature: String = "",
    ) : SFAccessToken() {
        fun getPostRequest(sObjectPath: String): Request =
            log.debug { "Doing getPostRequest with instance_url: $instance_url sObjectPath: $sObjectPath" }.let {
                Request(
                    Method.POST,
                    "$instance_url$sObjectPath",
                ).header("Authorization", "$token_type $access_token")
                    .header("Content-Type", "application/json;charset=UTF-8")
                    .also { log.debug { "Returning Request: $it" } }
            }
    }

    companion object {
        fun fromJson(data: String): SFAccessToken =
            runCatching { gson.fromJson(data, Exists::class.java) }
                .onFailure {
                    log.error { "Parsing of authorization response failed - ${it.localizedMessage}" }
                }.getOrDefault(Missing)
    }
}

data class KafkaMessage(
    val attributes: SFsObjectRestAttributes = SFsObjectRestAttributes(),
    val CRM_Topic__c: String,
    val CRM_Key__c: String,
    val CRM_Value__c: String,
)

data class SFsObjectRestAttributes(
    val type: String = "KafkaMessage__c",
)

/**
 * The general sObject REST API for posting records of different types
 * In this case, post of KafkaMessage containing attribute refering to Salesforce custom object KafkaMessage__c
 */
data class SFsObjectRest(
    val allOrNone: Boolean = true,
    val records: List<KafkaMessage>,
) {
    fun toJson(): String = gson.toJson(this)
}
