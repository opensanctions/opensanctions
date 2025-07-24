"use client";

import CodeMirror from "@uiw/react-codemirror";
import { EditorView } from "codemirror";
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import TurndownService from 'turndown';
import { markdown } from "@codemirror/lang-markdown";


export default function SourceView({ sourceValue, sourceContentType, sourceLabel }: { sourceValue: string, sourceContentType: string, sourceLabel: string }) {
  const tabs: React.ReactNode[] = [];
  const tab = (title: string, content: React.ReactNode | null = null) => {
    return <Tab key={title} eventKey={title} title={title} >
      {content ? content : <CodeMirror
        value={sourceValue}
        height="100%"
        width="100%"
        editable={false}
        style={{ height: '100%', width: '100%' }}
        extensions={[EditorView.lineWrapping]}
        title={sourceLabel}
      />}
    </Tab>
  }

  if (sourceContentType === 'text/html') {
    tabs.push(tab("As web page",
      <div
        style={{ height: '100%', width: '100%', overflow: 'auto', backgroundColor: '#fff', padding: '10px' }}
        dangerouslySetInnerHTML={{ __html: sourceValue }} />
    ))

    const turndownService = new TurndownService();
    tabs.push(tab("As Markdown",
      <CodeMirror
        value={turndownService.turndown(sourceValue)}
        height="100%"
        width="100%"
        editable={false}
        style={{ height: '100%', width: '100%' }}
        extensions={[markdown(), EditorView.lineWrapping]}
        title={sourceLabel}
      />
    ))

    tabs.push(tab("Original HTML"))
  } else if (sourceContentType === 'text/plain') {
    tabs.push(tab("Original Plain Text"))
  } else if (sourceContentType === 'image/png') {
    tabs.push(tab("Original PNG", <img src={sourceValue} alt={sourceLabel} />))
  } else {
    tabs.push(tab(sourceContentType))
  }
  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <Tabs className="flex-shrink-0">
        {tabs}
      </Tabs>
    </div>
  )
}
