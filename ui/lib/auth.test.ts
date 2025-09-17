describe('verify', () => {
  beforeEach(() => {
    // Mock fetch globally to return test public keys
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        'whatever': '-----BEGIN PUBLIC KEY-----\nwhatever\n-----END PUBLIC KEY-----\n'
      })
    });
    process.env.ZAVOD_ALLOW_UNAUTHENTICATED = 'false';
  });

  it('returns email for valid token', async () => {
    jest.resetModules();
    jest.doMock('jose', () => {
      return {
        jwtVerify: jest.fn().mockResolvedValue({ payload: { email: 'user@example.com' } }),
        importSPKI: jest.fn().mockResolvedValue({ type: 'public' }),
        exportJWK: jest.fn().mockResolvedValue({ kty: 'EC', crv: 'P-256' }),
        decodeProtectedHeader: jest.fn().mockReturnValue({ kid: 'whatever' }),
      };
    });
    // Re-import after mocking
    const { verify } = await import('./auth');
    const headers = new Headers({
      'x-goog-iap-jwt-assertion': 'a.jwt.token',
    });
    process.env.ZAVOD_IAP_AUDIENCE = 'test-audience';
    const email = await verify(headers);
    expect(email).toBe('user@example.com');
  });

  it('returns null if jwtVerify rejects', async () => {
    jest.resetModules();
    jest.doMock('jose', () => {
      return {
        jwtVerify: jest.fn().mockRejectedValue(new Error('JWT verification failed')),
        importSPKI: jest.fn().mockResolvedValue({ type: 'public' }),
        exportJWK: jest.fn().mockResolvedValue({ kty: 'EC', crv: 'P-256' }),
        decodeProtectedHeader: jest.fn().mockReturnValue({ kid: 'whatever' }),
      };
    });
    // Re-import after mocking
    const { verify } = await import('./auth');
    const headers = new Headers({
      'x-goog-iap-jwt-assertion': 'a.jwt.token',
    });
    process.env.ZAVOD_IAP_AUDIENCE = 'test-audience';
    const email = await verify(headers);
    expect(email).toBeNull();
  });
});
