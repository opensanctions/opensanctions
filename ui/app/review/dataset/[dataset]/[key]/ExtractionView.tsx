'use client';

import { json } from "@codemirror/lang-json";
import { syntaxTree } from "@codemirror/language";
import { keymap } from '@codemirror/view';
import CodeMirror, { EditorView } from '@uiw/react-codemirror';
import { createHighlighter } from '@/lib/codemirror';
import { yamlSchema } from "codemirror-json-schema/yaml";
import { Draft04, JsonSchema } from 'json-schema-library';
import { parse as parseYaml, stringify as stringifyToYaml } from 'yaml';
import React, { useState, useEffect, useRef } from 'react';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
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
    Accept &gt; <span className="text-muted" style={{ fontSize: '0.8em' }}></span>
  </button>
  const tooltip = <Tooltip id="accept-continue-tooltip">
    {isValid ? (
      <>&quot;Ctrl/Cmd+Enter&quot;<br />
        Accept and continue to next review</>
    ) : help}

  </Tooltip>
  return (
    <OverlayTrigger overlay={tooltip}>
      {/* span to give the trigger a non-disabled element to attach
  to for the disabled button's pointer events to trigger the tooltip.*/}
      <span>{button}</span>
    </OverlayTrigger>
  );
}

function searchSelectedValue(state: EditorView["state"], search: (query: string) => void) {
  const pos = state.selection.main.head;
  const tree = syntaxTree(state)
  const node = tree?.resolveInner(pos);
  // Don't search if we clicked outside a string value
  if (!["String", "Literal", "QuotedLiteral"].includes(node?.type.name || "")) {
    console.log("Not a string or literal node", node?.type.name);
    return;
  }
  const selection = state.selection;
  const mainRange = selection.ranges[selection.mainIndex];

  // Don't search if we've selected a value. We're probably trying to edit it
  // and search might be stealing focus.
  // It might be nice to be able to search for a partial string by selecting it,
  // and this is blocking that.
  if (mainRange.from !== mainRange.to) {
    return;
  }
  const nodeText = state.doc.sliceString(node.from, node.to);
  // Strip both single and double quotes from the start and end
  const unquotedText = nodeText.replace(/^"|"$/g, "");
  search(unquotedText);
}

interface ExtractionViewProps {
  rawData: unknown;
  extractedData: unknown;
  schema: JsonSchema;
  accepted: boolean;
  entryKey: string;
  dataset: string;
  search: (query: string) => void;
  highlightQuery?: string;
}

export default function ExtractionView({ rawData, extractedData, schema, accepted: initialAccepted, entryKey, dataset, search, highlightQuery }: ExtractionViewProps) {
  const [accepted, setAccepted] = useState(initialAccepted);
  const [editorExtracted, setEditorExtracted] = useState(stringifyToYaml(extractedData, null, { indent: 2, lineWidth: 0 }));
  const [flashInvalid, setFlashInvalid] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const editorRef = useRef<any>(null);

  function triggerFlash() {
    setFlashInvalid(true);
    setTimeout(() => setFlashInvalid(false), 150);
    setTimeout(() => setFlashInvalid(true), 300);
    setTimeout(() => setFlashInvalid(false), 450);
  }

  const schemaNode = new Draft04(schema);  // version selected to match that used in codemirror-json-schema

  let errors: string[];
  try {
    const editorExtractedParsed = parseYaml(editorExtracted)
    errors = schemaNode.validate(editorExtractedParsed).map(e => e.message);
  } catch (e) {
    if (e instanceof SyntaxError) {
      errors = [e.toString()];
    } else {
      errors = ['Unknown validation error'];
    }
  }

  const valid = errors.length === 0;
  const errorSummary = errors.length === 0 ? null : `${errors.length} error(s) in Extracted YAML: ${errors.join('; ')}`;

  // Keyboard shortcuts
  useEffect(() => {
    function handler(e: KeyboardEvent) {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'e') {
        e.preventDefault();
        // Focus the CodeMirror editor
        editorRef.current?.view?.focus();
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        if (valid) {
          formRef.current?.requestSubmit();
        } else {
          triggerFlash();
        }
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        if (valid) {
          if (formRef.current) {
            (formRef.current.accepted as HTMLInputElement).value = 'true';
            (formRef.current.accept_and_continue as HTMLInputElement).value = 'true';
            formRef.current.requestSubmit();
          }
        } else {
          triggerFlash();
        }
      }
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 't') {
        e.preventDefault();
        setEditorExtracted(stringifyToYaml(rawData, null, { indent: 2, lineWidth: 0 }));
      }
    }
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [valid, rawData]);
  useEffect(() => {
    const element = editorRef.current?.editor;
    function handler() {
      const editor = editorRef.current;
      if (!editor || !editor.view || !editor.view.state) {
        console.log("No editor or state", editor);
        return;
      }
      searchSelectedValue(editor.view.state, search);
    }
    element.addEventListener('click', handler);
    return () => element.removeEventListener('click', handler);
  }, [search])


  const escapeBlurKeymap = keymap.of([
    {
      key: 'Escape',
      run: (view) => {
        view.contentDOM.blur();
        return true;
      },
    },
  ]);

  const highlighter = createHighlighter(highlightQuery || '');

  return (
    <div className="entry-tabs flex-grow-1 d-flex flex-column" style={{ height: '100%' }}>
      <div className="d-flex flex-column" style={{ minHeight: 0, height: '100%' }}>
        <Tabs defaultActiveKey="extracted" className="flex-shrink-0">
          <Tab eventKey="raw" title="Original extraction">
            <div style={{ height: '100%' }}>
              <CodeMirror
                value={stringifyToYaml(rawData, null, { indent: 2, lineWidth: 0 })}
                extensions={[
                  yamlSchema(schema),
                  EditorView.lineWrapping,
                  ...(highlighter ? [highlighter] : [])
                ]}
                editable={false}
                height="100%"
                style={{ height: '100%', width: '100%' }}
                width="100%"
              />
            </div>
          </Tab>
          <Tab eventKey="extracted" title="Edit extracted data">
            <div style={{ height: '100%' }} className={flashInvalid ? 'editor-flash-invalid' : ''}>
              <CodeMirror
                ref={editorRef}
                value={editorExtracted}
                extensions={[
                  yamlSchema(schema),
                  EditorView.lineWrapping,
                  escapeBlurKeymap,
                  ...(highlighter ? [highlighter] : [])
                ]}
                height="100%"
                style={{ height: '100%' }}
                width="100%"
                onChange={setEditorExtracted}
                title="Press ctrl/cmd+e to jump to editor, or escape to be able to tab to the next field"
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
          <OverlayTrigger overlay={<Tooltip id="reset-tooltip">
            Ctrl/Cmd+t<br />
            Reset to original extraction
          </Tooltip>}>
            <span>
              <button
                type="button"
                className="btn btn-outline-secondary me-2"
                onClick={() => setEditorExtracted(stringifyToYaml(rawData, null, { indent: 2, lineWidth: 0 }))}
              >
                Reset
              </button>
            </span>
          </OverlayTrigger>

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
          <SaveButton isValid={valid} help={errorSummary} />
          <AcceptAndContinueButton isValid={valid} help={errorSummary} />
        </form>
      </div>
    </div>
  );
}
