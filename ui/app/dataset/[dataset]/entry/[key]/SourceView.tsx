"use client";

import CodeMirror from "@uiw/react-codemirror";
import { EditorView } from "codemirror";
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import TurndownService from 'turndown';
import { markdown } from "@codemirror/lang-markdown";
import Image from 'next/image'


export default function SourceView({ sourceValue, sourceContentType, sourceLabel }: { sourceValue: string, sourceContentType: string, sourceLabel: string }) {
  let tabTitle
  if (sourceContentType === 'text/html') {
    tabTitle = 'Original HTML';
  } else if (sourceContentType === 'text/plain') {
    tabTitle = 'Original Plain Text';
  } else if (sourceContentType === 'image/png') {
    tabTitle = 'Original PNG';
  } else {
    tabTitle = sourceContentType;
  }
  var turndownService = new TurndownService();
  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <Tabs defaultActiveKey="source" className="flex-shrink-0">
        <Tab eventKey="source" title={tabTitle}>
          {sourceContentType === 'image/png' ? (
            <img src={sourceValue} alt={sourceLabel} width={"100%"} title={sourceLabel}/>
          ) : (
          <CodeMirror
            value={sourceValue}
            height="100%"
            width="100%"
            editable={false}
            style={{ height: '100%', width: '100%' }}
            extensions={[EditorView.lineWrapping]}
            title={sourceLabel}
          />
          )}
        </Tab>
        {sourceContentType === 'text/html' && (
          <Tab eventKey="markdown" title="Converted to Markdown">
            <CodeMirror
              value={turndownService.turndown(sourceValue)}
              height="100%"
              width="100%"
              editable={false}
              style={{ height: '100%', width: '100%' }}
              extensions={[ markdown(), EditorView.lineWrapping]}
              title={sourceLabel}
            />
          </Tab>
        )}
      </Tabs>
    </div>
  )
}
