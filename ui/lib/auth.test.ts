import { verify } from './auth';

describe('verify', () => {
  it('returns email for valid token', async () => {
    jest.resetModules();
    jest.doMock('google-auth-library', () => {
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
    // Re-import after mocking
    const { verify } = require('./auth');
    const headers = new Headers({
      'x-goog-iap-jwt-assertion': 'a.jwt.token',
    });
    process.env.ZAVOD_IAP_AUDIENCE = 'test-audience';
    const email = await verify(headers);
    expect(email).toBe('user@example.com');
  });

  it('returns null if verifySignedJwtWithCertsAsync throws', async () => {
    jest.resetModules();
    jest.doMock('google-auth-library', () => {
      return {
        OAuth2Client: jest.fn().mockImplementation(() => ({
          getIapPublicKeys: jest.fn().mockResolvedValue({}),
          verifySignedJwtWithCertsAsync: jest.fn().mockImplementation(() => {
            throw new Error('JWT verification failed');
          }),
        }))
      };
    });
    // Re-import after mocking
    const { verify } = require('./auth');
    const headers = new Headers({
      'x-goog-iap-jwt-assertion': 'a.jwt.token',
    });
    process.env.ZAVOD_IAP_AUDIENCE = 'test-audience';
    const email = await verify(headers);
    expect(email).toBeNull();
  });
});
