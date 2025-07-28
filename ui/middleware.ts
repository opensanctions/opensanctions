import { verify } from './lib/auth';
import { NextRequest, NextResponse } from 'next/server';

export const config = {
  matcher: [
    '/((?!readyz))',
  ],
};

export default async function middleware(req: NextRequest) {
  const email = await verify(req.headers);
  if (!email) {
    return new NextResponse(
      '<h1>Error: Unauthorized</h1>',
      { status: 401, headers: { 'Content-Type': 'text/html' } }
    );
  }
  return NextResponse.next()
}
