"use client";

import CodeMirror from "@uiw/react-codemirror";
import { EditorView } from "codemirror";
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import TurndownService from 'turndown';
import { markdown } from "@codemirror/lang-markdown";


export default function SourceView({ sourceValue, sourceContentType, sourceLabel }: { sourceValue: string, sourceContentType: string, sourceLabel: string }) {
  const tabs: React.ReactNode[] = [];
  const defaultViewer = <CodeMirror
    value={sourceValue}
    height="100%"
    width="100%"
    editable={false}
    style={{ height: '100%', width: '100%' }}
    extensions={[EditorView.lineWrapping]}
    title={sourceLabel}
  />;

  if (sourceContentType === 'text/html') {
    tabs.push(<Tab eventKey="rendered" title="As web page">
      <div
      style={{ height: '100%', width: '100%', overflow: 'auto', backgroundColor: '#fff', padding: '10px' }}
      dangerouslySetInnerHTML={{ __html: sourceValue }} />
    </Tab>)
    var turndownService = new TurndownService();
    tabs.push(<Tab eventKey="markdown" title="As Markdown">
      <CodeMirror
        value={turndownService.turndown(sourceValue)}
        height="100%"
        width="100%"
        editable={false}
        style={{ height: '100%', width: '100%' }}
        extensions={[markdown(), EditorView.lineWrapping]}
        title={sourceLabel}
      />
    </Tab>)
    tabs.push(<Tab eventKey="source" title="Original HTML">{defaultViewer}</Tab>)
  } else if (sourceContentType === 'text/plain') {
    tabs.push(<Tab eventKey="source" title="Original Plain Text">{defaultViewer}</Tab>)
  } else if (sourceContentType === 'image/png') {
    tabs.push(<Tab eventKey="source" title="Original PNG"><img src={sourceValue} alt={sourceLabel} /></Tab>)
  } else {
    tabs.push(<Tab eventKey="source" title={sourceContentType}>{defaultViewer}</Tab>)
  }
  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <Tabs className="flex-shrink-0">
        {tabs}
      </Tabs>
    </div>
  )
}
