import { getExtractionEntry } from '@/lib/db';
import { verify } from '@/lib/auth';
import { headers } from 'next/headers';
import { notFound } from 'next/navigation';
import React from 'react';
import ExtractionView from './ExtractionView';
import Link from 'next/link';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import { BoxArrowUpRight } from 'react-bootstrap-icons';
import SourceView from './SourceView';
import { PageProps } from '@/lib/pageProps';

export default async function EntryPage({ params }: PageProps) {
  // Check authorization
  const headersList = await headers();
  const email = await verify(headersList);

  if (!email) {
    return (
      <div className="container-fluid vh-100 d-flex flex-column justify-content-center align-items-center p-4 bg-light">
        <div className="text-center">
          <h1 className="text-danger mb-4">Access Denied</h1>
          <p className="lead mb-3">You are not authorized to access this application.</p>
          <p className="text-muted">Please contact your administrator if you believe this is an error.</p>
        </div>
      </div>
    );
  }

  const awaitedParams = await params;
  const dataset = decodeURIComponent(awaitedParams.dataset);
  const key = decodeURIComponent(awaitedParams.key);
  const entry = await getExtractionEntry(dataset, key);
  if (!entry) return notFound();


  return (
    <div className="p-4 bg-light d-flex flex-column" style={{ height: '100vh', minHeight: 0 }}>
      <nav aria-label="breadcrumb" className="mb-3">
        <ol className="breadcrumb">
          <li className="breadcrumb-item">
            <Link href="/">Home</Link>
          </li>
          <li className="breadcrumb-item">
            <Link href={`/dataset/${encodeURIComponent(dataset)}`}>{dataset}</Link>
          </li>
          <li className="breadcrumb-item active" aria-current="page">
            Key: {key.length > 60
              ? `${key.slice(0, 30)}...${key.slice(-30)}`
              : key}
          </li>
        </ol>
      </nav>
      <div className="flex-grow-1 d-flex flex-column mb-4" style={{ minHeight: 0 }}>
        <Row style={{ height: '100%' }}>
          <Col className="d-flex flex-column" style={{ height: '100%' }}>
            <h2 className="h5 text-break d-flex">
              Source data
              {entry.source_url && (
                <span className="h6 d-flex gap-1">
                  &nbsp;
                  <a
                    href={entry.source_url}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                   <BoxArrowUpRight />
                  </a>
                </span>
              )}
            </h2>
            <SourceView sourceValue={entry.source_value} sourceContentType={entry.source_content_type} sourceLabel={entry.source_label} />
          </Col>
          <Col className="d-flex flex-column" style={{ height: '100%' }}>
            <h2 className="h5 text-break d-flex">Extracted data</h2>

            <ExtractionView
              rawData={entry.orig_extraction_data}
              extractedData={entry.extracted_data}
              schema={entry.extraction_schema}
              //schemaNode={schemaNode}
              accepted={entry.accepted}
              entryKey={key}
              dataset={dataset}
            />
          </Col>
        </Row>
      </div>
    </div >
  );
}
