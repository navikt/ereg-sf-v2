package no.nav.ereg.salesforce

interface AccessTokenHandler {
    val accessToken: String
    val instanceUrl: String
    val tenantId: String
}
