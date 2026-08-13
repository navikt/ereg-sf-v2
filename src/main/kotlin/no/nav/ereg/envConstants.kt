@file:Suppress("ktlint:standard:filename", "ktlint:standard:property-naming")

package no.nav.ereg

const val env_AZURE_APP_WELL_KNOWN_URL = "AZURE_APP_WELL_KNOWN_URL"
const val env_AZURE_APP_CLIENT_ID = "AZURE_APP_CLIENT_ID"
const val env_AZURE_APP_CLIENT_SECRET = "AZURE_APP_CLIENT_SECRET"
const val env_AZURE_OPENID_CONFIG_TOKEN_ENDPOINT = "AZURE_OPENID_CONFIG_TOKEN_ENDPOINT"

const val env_NAIS_CLUSTER_NAME = "NAIS_CLUSTER_NAME"

const val config_SALESFORCE_API_VERSION = "SALESFORCE_API_VERSION"
const val config_SF_TOKEN_HOST = "SF_TOKEN_HOST"
const val config_SF_JWT_USERNAME = "SF_JWT_USERNAME"

const val secret_SF_JWT_CLIENT_ID = "SF_JWT_CLIENT_ID"
const val secret_SF_JWT_KEYSTORE_B64 = "SF_JWT_KEYSTORE_B64"
const val secret_SF_JWT_KEYSTORE_PASSWORD = "SF_JWT_KEYSTORE_PASSWORD"

/**
 * Shortcut for fetching environment variables
 */
fun env(name: String): String = System.getenv(name) ?: throw NullPointerException("Missing env $name")
