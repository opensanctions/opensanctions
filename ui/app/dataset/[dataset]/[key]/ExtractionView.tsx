'use client';

import React, { useState, useEffect, useRef } from 'react';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import CodeMirror, { EditorView } from '@uiw/react-codemirror';
import { jsonSchema } from "codemirror-json-schema";
import { json } from "@codemirror/lang-json";
import { Draft04 } from 'json-schema-library';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';


function SaveButton({ isValid, help }: { isValid: boolean, help: string | null }) {
  const button = <button className="btn btn-primary ms-2" type="submit" disabled={!isValid} style={isValid ? {} : { pointerEvents: 'none' }}>
    Save <span className="text-muted" style={{ fontSize: '0.8em' }}></span>
  </button>;
  return (
    <OverlayTrigger overlay={<Tooltip id="save-tooltip">{isValid ? "Ctrl/Cmd+s" : help}</Tooltip>}>
      {/* span to give the trigger a non-disabled element to attach
      to for the disabled button's pointer events to trigger the tooltip.*/}
      <span>{button}</span>
    </OverlayTrigger>
  );
}

function AcceptAndContinueButton({ isValid, help }: { isValid: boolean, help: string | null }) {
  const onClick = (e: React.MouseEvent<HTMLButtonElement>) => {
    const form = e.currentTarget.form;
    if (form) {
      (form.accepted as HTMLInputElement).value = 'true';
      (form.accept_and_continue as HTMLInputElement).value = 'true';
    }
  }
  const button = <button
    className="btn btn-success ms-2"
    type="submit"
    onClick={onClick}
    disabled={!isValid}
    style={isValid ? {} : { pointerEvents: 'none' }}
  >
    Accept and continue <span className="text-muted" style={{ fontSize: '0.8em' }}></span>
  </button>
  return (
    <OverlayTrigger overlay={<Tooltip id="accept-continue-tooltip">{isValid ? "Ctrl/Cmd+Enter" : help}</Tooltip>}>
      {/* span to give the trigger a non-disabled element to attach
  to for the disabled button's pointer events to trigger the tooltip.*/}
      <span>{button}</span>
    </OverlayTrigger>
  );
}

interface ExtractionViewProps {
  rawData: any;
  extractedData: any;
  schema: any;
  accepted: boolean;
  entryKey: string;
  dataset: string;
}

export default function ExtractionView({ rawData, extractedData, schema, accepted: initialAccepted, entryKey, dataset }: ExtractionViewProps) {
  const [accepted, setAccepted] = useState(initialAccepted);
  const [editorExtracted, setEditorExtracted] = useState(JSON.stringify(extractedData, null, 2));
  const formRef = useRef<HTMLFormElement>(null);

  // Keyboard shortcuts
  useEffect(() => {
    function handler(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'e') {
        e.preventDefault();
        setAccepted(a => !a);
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        // Save
        formRef.current?.requestSubmit();
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        // Accept and continue
        if (formRef.current) {
          (formRef.current.accepted as HTMLInputElement).value = 'true';
          (formRef.current.accept_and_continue as HTMLInputElement).value = 'true';
          formRef.current.requestSubmit();
        }
      }
    }
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  const schemaNode = new Draft04(schema);  // version selected to match that used in codemirror-json-schema

  let errors: string[];
  try {
    const editorExtractedParsed = JSON.parse(editorExtracted)
    errors = schemaNode.validate(editorExtractedParsed).map(e => e.message);
  } catch (e) {
    if (e instanceof SyntaxError) {
      errors = [e.toString()];
    } else {
      errors = ['Unknown validation error'];
    }
  }

  const valid = errors.length === 0;
  const errorSummary = errors.length === 0 ? null : `${errors.length} error(s) in Extracted JSON: ${errors.join('; ')}`;

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
                value={editorExtracted}
                extensions={[jsonSchema(schema), EditorView.lineWrapping]}
                height="100%"
                style={{ height: '100%' }}
                width="100%"
                onChange={setEditorExtracted}
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
          ref={formRef}
          method="POST"
          action="/api/extraction/save"
          className="d-flex justify-content-end align-items-center mt-3 gap-3 w-100"
        >
          <input type="hidden" name="dataset" value={dataset} />
          <input type="hidden" name="key" value={entryKey} />
          <input type="hidden" name="extractedData" value={editorExtracted} />
          <input type="hidden" name="accepted" value={accepted ? 'true' : 'false'} />
          <input type="hidden" name="accept_and_continue" value="false" />
          <OverlayTrigger overlay={<Tooltip id="accepted-tooltip">Ctrl/Cmd+e</Tooltip>}>
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
          </OverlayTrigger>
          <SaveButton isValid={valid} help={errorSummary} />
          <AcceptAndContinueButton isValid={valid} help={errorSummary} />
        </form>
      </div>
    </div>
  );
}
