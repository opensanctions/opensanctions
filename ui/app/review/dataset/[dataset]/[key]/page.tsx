import Link from 'next/link';
import { notFound } from 'next/navigation';
import React from 'react';
import Row from 'react-bootstrap/Row';

import { getExtractionEntry } from '@/lib/db';
import { PageProps } from '@/lib/pageProps';

import DataView from './DataView';

export default async function EntryPage({ params }: PageProps) {
  const awaitedParams = await params;
  const dataset = decodeURIComponent(awaitedParams.dataset);
  const key = decodeURIComponent(awaitedParams.key);
  const entry = await getExtractionEntry(dataset, key);

  if (!entry) return notFound();


  return (
    <div className="p-4 d-flex flex-column" style={{ height: 'calc(100vh - 50px)', minHeight: 0 }}>
      <nav aria-label="breadcrumb" className="mb-3">
        <ol className="breadcrumb">
          <li className="breadcrumb-item">
            <Link href="/review">Reviews</Link>
          </li>
          <li className="breadcrumb-item">
            <Link href={`/review/dataset/${encodeURIComponent(dataset)}`}>{dataset}</Link>
          </li>
          <li className="breadcrumb-item" aria-current="page">
            Key: {key.length > 60
              ? `${key.slice(0, 30)}...${key.slice(-30)}`
              : key}
          </li>
        </ol>
      </nav>
      <div className="flex-grow-1 d-flex flex-column mb-4" style={{ minHeight: 0 }}>
        <Row style={{ height: '100%' }}>
          <DataView dataset={dataset} entryKey={key} entry={entry} />
        </Row>
      </div>
    </div >
  );
}
