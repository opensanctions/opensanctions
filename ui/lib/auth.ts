// https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs

import { OAuth2Client } from 'google-auth-library';

const oAuth2Client = new OAuth2Client();
// Expected Audience for App Engine.
// expectedAudience = `/projects/${projectNumber}/apps/${projectId}`;
// Expected Audience for Compute Engine, Cloud Run looks the same
// expectedAudience = `/projects/${projectNumber}/global/backendServices/${backendServiceId}`;
const expectedAudience = process.env.ZAVOD_IAP_AUDIENCE;

export async function verify(headers: Headers) {
    if (process.env.ZAVOD_ALLOW_UNAUTHENTICATED === 'true') {
        return "anonymous"
    }

    // Verify the id_token, and access the claims.
    const iapJwt = headers.get('x-goog-iap-jwt-assertion');
    if (!iapJwt) {
        console.log(
            "No 'x-goog-iap-jwt-assertion' header in request",
            { ip: headers.get('x-forwarded-for') }
        );
        return null;
    }
    const response = await oAuth2Client.getIapPublicKeys();
    try {
        const ticket = await oAuth2Client.verifySignedJwtWithCertsAsync(
            iapJwt,
            response.pubkeys,
            expectedAudience!, // exclamation asserts non-null
            ['https://cloud.google.com/iap'],
        );
        return ticket.getPayload()?.email;
    } catch (error) {
        console.error(error, iapJwt);
    }
    return null;
}


