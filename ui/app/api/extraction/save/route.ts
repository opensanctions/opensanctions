import { NextRequest, NextResponse } from 'next/server';
import { updateExtractionEntry, getNextUnacceptedEntryKey } from '../../../../lib/db';

export async function POST(req: NextRequest) {
  let dataset: string, key: string, accepted: boolean, extractedData: string, acceptAndContinue: boolean;
  const contentType = req.headers.get('content-type') || '';
  if (contentType.includes('application/x-www-form-urlencoded')) {
    const formData = await req.formData();
    dataset = (formData.get('dataset') as string) || '';
    key = (formData.get('key') as string) || '';
    accepted = formData.get('accepted') === 'true';
    extractedData = (formData.get('extractedData') as string) || '';
    acceptAndContinue = formData.get('accept_and_continue') === 'true';
  } else {
    return new Response('<h1>Error: Unsupported content type</h1>', { status: 400, headers: { 'Content-Type': 'text/html' } });
  }
  try {
    await updateExtractionEntry({ dataset, key, accepted, extractedData: JSON.parse(extractedData) as object });
    let redirectUrl = `/dataset/${encodeURIComponent(dataset)}`;
    if (acceptAndContinue) {
      const nextKey = await getNextUnacceptedEntryKey(dataset);
      if (nextKey) {
        redirectUrl = `/dataset/${encodeURIComponent(dataset)}/entry/${encodeURIComponent(nextKey)}`;
      }
    }
    return NextResponse.redirect(new URL(redirectUrl, req.nextUrl.origin));
  } catch (e: any) {
    // Log the error but do not expose details to the user
    console.error('Extraction save error:', e);
    return new Response('<h1>An error occurred while saving. Please try again later.</h1>', { status: 500, headers: { 'Content-Type': 'text/html' } });
  }
}
