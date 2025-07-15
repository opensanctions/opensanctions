import { verify } from './auth';

// Assume they know what they're doing and just mock the verification.
jest.mock('google-auth-library', () => {
  return {
    OAuth2Client: jest.fn().mockImplementation(() => ({
      getIapPublicKeys: jest.fn().mockResolvedValue({}),
      verifySignedJwtWithCertsAsync: jest.fn().mockImplementation((token, pubkeys, audience, issuers) => {
        return {
          getPayload: () => ({ email: 'user@example.com' })
        };
      })
    }))
  };
});

describe('verify', () => {
  it('returns email for valid token', async () => {
    const req = new Request('http://localhost', {
      headers: {
        'x-goog-iap-jwt-assertion': 'valid.jwt.token',
      },
    });
    process.env.ZAVOD_IAP_AUDIENCE = 'test-audience';
    const email = await verify(req);
    expect(email).toBe('user@example.com');
  });
});
