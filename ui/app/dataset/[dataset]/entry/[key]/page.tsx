import { getExtractionEntry } from '../../../../../lib/db';
import { notFound } from 'next/navigation';
import React from 'react';
import ExtractionView from './ExtractionView';
import Link from 'next/link';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import SourceView from './SourceView';


export default async function EntryPage(props: { params: { dataset: string, key: string } }) {
  const params = await props.params;
  const dataset = decodeURIComponent(params.dataset);
  const key = decodeURIComponent(params.key);
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
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h1 className="h4 text-break mb-0">Review extraction</h1>
        <a
          href={entry.source_url}
          className="link-primary fw-semibold fs-5"
          target="_blank"
          rel="noopener noreferrer"
        >
          View Source
        </a>
      </div>
      <div className="flex-grow-1 d-flex flex-column mb-4" style={{ minHeight: 0 }}>
        <Row style={{ height: '100%' }}>
          <Col className="d-flex flex-column"  style={{ height: '100%' }}>
            <h2 className="h5 text-break d-flex">Source data</h2>
            <SourceView sourceValue={entry.source_value} sourceContentType={entry.source_content_type} sourceLabel={entry.source_label} />
          </Col>
          <Col className="d-flex flex-column" style={{ height: '100%' }}>
            <h2 className="h5 text-break d-flex">Extracted data</h2>

            <ExtractionView
              rawData={entry.raw_data}
              extractedData={entry.extracted_data}
              schema={entry.schema}
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
