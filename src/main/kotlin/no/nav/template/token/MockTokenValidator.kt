package no.nav.template.token

import no.nav.security.token.support.core.jwt.JwtToken
import org.http4k.core.Request

class MockTokenValidator : TokenValidator {
    override fun firstValidToken(request: Request): JwtToken? {
        TODO("Not yet implemented")
    }
}
