import { NextRequest, NextResponse } from 'next/server';

import { verify } from '../../../../lib/auth';
import { BASE_URL } from '../../../../lib/constants';
import { updateExtractionEntry, getNextUnacceptedEntryKey } from '../../../../lib/db';
import { parse as parseYaml } from 'yaml';

function getAsString(formData: FormData, key: string) {
  const value = formData.get(key);
  if (value === null) {
    throw new Error(`Form data missing key: ${key}`);
  }
  return value as string;
}

function getAsBool(formData: FormData, key: string) {
  return getAsString(formData, key) === 'true';
}

export async function POST(req: NextRequest) {
  const email = await verify(req.headers);

  const contentType = req.headers.get('content-type') || '';
  if (!contentType.includes('application/x-www-form-urlencoded')) {
    return new Response('<h1>Error: Unsupported content type</h1>', { status: 400, headers: { 'Content-Type': 'text/html' } });
  }

  const formData = await req.formData();

  try {
    const dataset = getAsString(formData, 'dataset');
    const key = getAsString(formData, 'key');
    const accepted = getAsBool(formData, 'accepted');
    const extractedData = getAsString(formData, 'extractedData');
    const acceptAndContinue = getAsBool(formData, 'accept_and_continue');

    await updateExtractionEntry({
      dataset,
      key,
      accepted,
      extractedData: parseYaml(extractedData) as object,
      modifiedBy: email!,
    });

    let redirectUrl = `/review/dataset/${encodeURIComponent(dataset)}/${encodeURIComponent(key)}`;
    if (acceptAndContinue) {
      const nextKey = await getNextUnacceptedEntryKey(dataset);
      if (nextKey) {
        redirectUrl = `/review/dataset/${encodeURIComponent(dataset)}/${encodeURIComponent(nextKey)}`;
      }
    }
    return NextResponse.redirect(new URL(redirectUrl, BASE_URL));
  } catch (e: unknown) {
    // Log the error but do not expose details to the user
    console.error('Extraction save error:', e);
    return new Response('<h1>An error occurred while saving. Please try again later.</h1>', { status: 500, headers: { 'Content-Type': 'text/html' } });
  }
}
