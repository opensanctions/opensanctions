// https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs

import { OAuth2Client } from 'google-auth-library';

const oAuth2Client = new OAuth2Client();
// Expected Audience for App Engine.
// expectedAudience = `/projects/${projectNumber}/apps/${projectId}`;
// Expected Audience for Compute Engine
//expectedAudience = `/projects/${projectNumber}/global/backendServices/${backendServiceId}`;
const expectedAudience = process.env.ZAVOD_IAP_AUDIENCE;

export async function verify(headers: Headers) {
    if (process.env.ZAVOD_UNSAFE_IAP_AUTH_DISABLED === 'true') {
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
    const ticket = await oAuth2Client.verifySignedJwtWithCertsAsync(
        iapJwt,
        response.pubkeys,
        expectedAudience,
        ['https://cloud.google.com/iap'],
    );
    // TODO: Remove after initial deployment when we've seen a token for a positive test.
    console.log(ticket);
    console.log(ticket.getPayload())
    return ticket.getPayload()?.email;
}


