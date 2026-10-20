import { NextRequest, NextResponse } from 'next/server';

import { verify } from '../../../../lib/auth';
import { BASE_URL } from '../../../../lib/constants';
import { getNextEntryKey } from '../../../../lib/db';

export async function GET(req: NextRequest) {
  const email = await verify(req.headers);

  const dataset = req.nextUrl.searchParams.get('dataset');
  const cursorKey = req.nextUrl.searchParams.get('cursorKey');
  if (dataset === null || cursorKey === null) {
    return new Response('<h1>Error: dataset and cursorKey are required</h1>', { status: 400, headers: { 'Content-Type': 'text/html' } });
  }

  const nextKey = await getNextEntryKey(dataset, email!, cursorKey);
  const redirectKey = nextKey ?? cursorKey;
  const redirectUrl = `/review/dataset/${encodeURIComponent(dataset)}/${encodeURIComponent(redirectKey)}`;
  return NextResponse.redirect(new URL(redirectUrl, BASE_URL));
}
