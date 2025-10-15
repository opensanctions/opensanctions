"use client";

import React from 'react';
import Col from 'react-bootstrap/Col';
import { BoxArrowUpRight } from 'react-bootstrap-icons';

import { Review } from '@/lib/db';

import ExtractionView from './ExtractionView';
import SourceView from './SourceView';

export default function DataView({ entry, dataset, entryKey }: { entry: Review, dataset: string, entryKey: string }) {
  const [searchQuery, setSearchQuery] = React.useState('');

  return (
    <>
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
        <SourceView
          sourceValue={entry.source_value}
          sourceMimeType={entry.source_mime_type}
          sourceLabel={entry.source_label}
          searchQuery={searchQuery}
        />
      </Col>
      <Col className="d-flex flex-column" style={{ height: '100%' }}>
        <h2 className="h5 text-break d-flex">Extracted data</h2>

        <ExtractionView
          rawData={entry.original_extraction}
          extractedData={entry.extracted_data}
          schema={entry.extraction_schema}
          //schemaNode={schemaNode}
          accepted={entry.accepted}
          entryKey={entryKey}
          dataset={dataset}
          search={setSearchQuery}
        />
      </Col>
    </>
  )
}
