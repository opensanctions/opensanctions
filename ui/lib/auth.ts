import { OAuth2Client } from 'google-auth-library';
import jwt from 'jsonwebtoken';

const oAuth2Client = new OAuth2Client();
// Expected Audience for App Engine.
// expectedAudience = `/projects/${projectNumber}/apps/${projectId}`;
// Expected Audience for Compute Engine
//expectedAudience = `/projects/${projectNumber}/global/backendServices/${backendServiceId}`;
const expectedAudience = process.env.IAP_AUDIENCE;

async function verify(request: Request) {
    const iapAuthDisabled = process.env.IAP_AUTH_DISABLED === 'true';
    if (iapAuthDisabled) {
        return {
            email: null,
            name: 'Anonymous User',
        };
    }

    // Verify the id_token, and access the claims.
    const iapJwt = request.headers.get('x-goog-iap-jwt-assertion');
    if (!iapJwt) {
        return null;
    }
    const response = await oAuth2Client.getIapPublicKeys();
    const ticket = await oAuth2Client.verifySignedJwtWithCertsAsync(
      iapJwt,
      response.pubkeys,
      expectedAudience,
      ['https://cloud.google.com/iap'],
    );
    console.log(ticket);
  }

  verify().catch(console.error);
