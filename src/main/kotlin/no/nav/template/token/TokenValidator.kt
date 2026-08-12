package no.nav.template.token

import no.nav.security.token.support.core.jwt.JwtToken
import org.http4k.core.Request

interface TokenValidator {
    fun firstValidToken(request: Request): JwtToken?

//    fun hasTokenFromSalesforce(request: Request): Boolean
//
//    fun nameClaim(request: Request): String
//
//    fun expireTime(request: Request): Long
}
