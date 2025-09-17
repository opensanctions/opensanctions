import { NextResponse } from 'next/server';

import { getDb } from '../../lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const db = (await getDb());
    const result = await db
      .selectFrom('review')
      .select(({ fn }) => [fn.count('dataset').distinct().as('count')])
      .executeTakeFirst();
    const count = result?.count;
    return new NextResponse(`ok - ${count} datasets`, { status: 200 });

  } catch (e) {
    return new NextResponse('error - database query failed', { status: 500 });
  }
}
