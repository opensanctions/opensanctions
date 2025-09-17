import { NextRequest, NextResponse } from 'next/server';

import { verify } from '../../../../lib/auth';
import { softDeleteAndCreatePosition } from '../../../../lib/db';

interface PositionUpdateData {
  caption: string;
  countries: string[];
  is_pep: boolean | null;
  topics: string[];
  dataset: string;
}

export async function PUT(
  req: NextRequest,
  { params }: { params: Promise<{ entityId: string }> }
) {
  const email = await verify(req.headers);
  const { entityId } = await params;

  if (!(req.headers.get('content-type') || '').includes('application/json')) {
    return NextResponse.json(
      { error: 'Content-Type must be application/json' },
      { status: 400 }
    );
  }

  const data: PositionUpdateData = await req.json();

  // We're quite aggresive here, throwing lots of errors if something goes wrong
  // but not caching them. Oh well, this is internal, so 500 or 404 doesn't matter
  // all that much.
  const updatedPosition = await softDeleteAndCreatePosition({
    entityId,
    positionUpdate: data,
    modifiedBy: email!,
  });

  return NextResponse.json(updatedPosition);
}
