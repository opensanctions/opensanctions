import { POST } from './route';
import { NextRequest } from 'next/server';

jest.mock('../../../../lib/auth', () => ({
  verify: jest.fn().mockResolvedValue(null),
}));

describe('POST /api/extraction/save', () => {
  it('returns 401 for valid JWT header but unauthorized', async () => {
    // Create a mock request with a valid JWT header
    const form = new URLSearchParams({
      dataset: 'test',
      key: 'test',
      accepted: 'true',
      extractedData: '{}',
      accept_and_continue: 'false',
    });
    const req = new NextRequest('http://localhost', {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded',
        'x-goog-iap-jwt-assertion': 'valid.jwt.token',
      },
      body: form,
    } as any);

    const res = await POST(req);
    expect(res.status).toBe(401);
    const text = await res.text();
    expect(text).toContain('Unauthorized');
  });
});
