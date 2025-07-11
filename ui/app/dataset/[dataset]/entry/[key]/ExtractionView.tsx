'use client';

import React, { useState } from 'react';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import CodeMirror, { EditorView } from '@uiw/react-codemirror';
import { jsonSchema } from "codemirror-json-schema";
import { json } from "@codemirror/lang-json";


interface EntryTabsProps {
  rawData: any;
  extractedData: any;
  schema: any;
  accepted: boolean;
  entryKey: string;
  dataset: string;
}

export default function ExtractionView({ rawData, extractedData, schema, accepted: initialAccepted, entryKey, dataset }: EntryTabsProps) {
  const [accepted, setAccepted] = useState(initialAccepted);
  const [extracted, setExtracted] = useState(JSON.stringify(extractedData, null, 2));

  return (
    <div className="entry-tabs flex-grow-1 d-flex flex-column" style={{ height: '100%' }}>
      <div className="d-flex flex-column" style={{ minHeight: 0, height: '100%' }}>
        <Tabs defaultActiveKey="extracted" className="flex-shrink-0">
          <Tab eventKey="raw" title="Original extraction">
            <button>Reset to this version</button>
            <div style={{ height: '100%' }}>
              <CodeMirror
                value={JSON.stringify(rawData, null, 2)}
                extensions={[jsonSchema(schema), EditorView.lineWrapping]}
                editable={false}
                height="100%"
                style={{ height: '100%', width: '100%' }}
                width="100%"
              />
            </div>
          </Tab>
          <Tab eventKey="extracted" title="Extracted JSON">
            <div style={{ height: '100%' }}>
              <CodeMirror
                value={extracted}
                extensions={[jsonSchema(schema), EditorView.lineWrapping]}
                height="100%"
                style={{ height: '100%' }}
                width="100%"
                onChange={setExtracted}
              />
            </div>
          </Tab>
          <Tab eventKey="schema" title="Entry Schema">
            <div style={{ height: '100%' }}>
              <CodeMirror
                value={JSON.stringify(schema, null, 2)}
                extensions={[json(), EditorView.lineWrapping]}
                editable={false}
                height="100%"
                style={{ height: '100%', width: '100%' }}
                width="100%"
              />
            </div>
          </Tab>
        </Tabs>
        <form
          method="POST"
          action="/api/extraction/save"
          className="d-flex justify-content-end align-items-center mt-3 gap-3 w-100"
        >
          <input type="hidden" name="dataset" value={dataset} />
          <input type="hidden" name="key" value={entryKey} />
          <input type="hidden" name="extractedData" value={extracted} />
          <input type="hidden" name="accepted" value={accepted ? 'true' : 'false'} />
          <input type="hidden" name="accept_and_continue" value="false" />
          <div className="form-check">
            <input
              className="form-check-input"
              type="checkbox"
              id="acceptedCheck"
              checked={accepted}
              onChange={e => setAccepted(e.target.checked)}
            />
            <label className="form-check-label" htmlFor="acceptedCheck">
              Accepted
            </label>
          </div>
          <button className="btn btn-primary ms-2" type="submit">
            Save
          </button>
          <button
            className="btn btn-success ms-2"
            type="submit"
            onClick={e => {
              // Set accept to true and accept_and_continue to true before submit
              const form = e.currentTarget.form;
              if (form) {
                (form.accepted as HTMLInputElement).value = 'true';
                (form.accept_and_continue as HTMLInputElement).value = 'true';
              }
            }}
          >
            Accept and continue
          </button>
        </form>
      </div>
    </div>
  );
}
