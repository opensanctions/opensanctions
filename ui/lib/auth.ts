// https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs

import { jwtVerify, importSPKI, exportJWK, decodeProtectedHeader } from 'jose';

import { ALLOW_UNAUTHENTICATED, IAP_AUDIENCE } from './constants';

const CACHE_DURATION = 6 * 60 * 60; // 6 hours

async function fetchPublicKeys(): Promise<Record<string, string>> {
  const options: RequestInit = {
    cache: 'force-cache',
    next: {
      revalidate: CACHE_DURATION,
    },
  }
  const response = await fetch('https://www.gstatic.com/iap/verify/public_key', options);
  return await response.json();
}

export async function verify(headers: Headers) {
  if (ALLOW_UNAUTHENTICATED) {
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

  try {
    // Get public keys
    const publicKeys = await fetchPublicKeys();

    const claims = decodeProtectedHeader(iapJwt);
    const keyId = claims.kid

    if (!keyId || typeof keyId !== 'string') {
      throw new Error(`No valid key ID in JWT ${keyId}`);
    }

    const publicPem = publicKeys[keyId];
    if (!publicPem) {
      throw new Error(`Public key not found for key ID: ${keyId}`);
    }

    const publicKey = await importSPKI(publicPem, 'ES256', {extractable: true})
    const publicJwk = await exportJWK(publicKey)

    // Verify the JWT
    const {payload} = await jwtVerify(iapJwt, publicJwk, {
      issuer: 'https://cloud.google.com/iap',
      audience: IAP_AUDIENCE,
    });

    return payload.email as string;
  } catch (error) {
    console.error('JWT verification error:', error, iapJwt);
  }
  return null;
}

