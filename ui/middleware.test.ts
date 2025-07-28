import { NextRequest } from 'next/server';
import middleware from './middleware';

jest.mock('./lib/auth', () => ({
  verify: jest.fn().mockResolvedValue(null),
}));

describe('middleware', () => {
  it('responds 401 if unauthorised', async () => {
    // Create a mock request with a valid JWT header
    const form = new URLSearchParams({
      dataset: 'test',
      key: 'test',
      accepted: 'true',
      extractedData: '{}',
      accept_and_continue: 'false',
    });
    const req = new NextRequest('http://localhost/api/extraction/save', {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded',
        'x-goog-iap-jwt-assertion': 'a.jwt.token',
      },
      body: form,
    } as any);

    const res = await middleware(req);
    expect(res.status).toBe(401);
    const text = await res.text();
    expect(text).toContain('Unauthorized');
  });
});
