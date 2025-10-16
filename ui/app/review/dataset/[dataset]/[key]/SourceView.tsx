"use client";

import { markdown } from "@codemirror/lang-markdown";
import { openSearchPanel, setSearchQuery, SearchQuery } from "@codemirror/search";
import CodeMirror, { ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { EditorView } from "codemirror";
import { useEffect, useRef } from "react";
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import TurndownService from 'turndown';

type SourceViewProps = {
  sourceValue: string,
  sourceMimeType: string,
  sourceLabel: string,
  searchQuery: string
};

export default function SourceView({ sourceValue, sourceMimeType, sourceLabel, searchQuery }: SourceViewProps) {

  const markdownRef = useRef<ReactCodeMirrorRef>(null);

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

  if (sourceMimeType === 'text/html') {
    let highlighted: string;
    if (!!searchQuery) {
      highlighted = sourceValue.replace(new RegExp(searchQuery, 'gi'), (match) => `<mark>${match}</mark>`);
    } else {
      highlighted = sourceValue;
    }
    tabs.push(tab("As web page",
      <div
        style={{ height: '100%', width: '100%', overflow: 'auto', backgroundColor: '#fff', padding: '10px' }}
        dangerouslySetInnerHTML={{ __html: highlighted }} />
    ))

    const turndownService = new TurndownService();
    tabs.push(tab("As Markdown",
      <CodeMirror
        ref={markdownRef}
        value={turndownService.turndown(sourceValue)}
        height="100%"
        width="100%"
        editable={false}
        style={{ height: '100%', width: '100%' }}
        extensions={[
          markdown(),
          EditorView.lineWrapping,
          // Allow setting focus to allow opening search panel despite being read-only
          EditorView.contentAttributes.of({ tabindex: "0" }),
        ]}
        title={sourceLabel}
        basicSetup={{
          searchKeymap: true,
        }}
      />
    ));

    tabs.push(tab("Original HTML"))
  } else if (sourceMimeType === 'text/plain') {
    tabs.push(tab("Original Plain Text"))
  } else if (sourceMimeType === 'image/png') {
    tabs.push(tab("Original PNG", <img src={sourceValue} alt={sourceLabel} />))
  } else {
    tabs.push(tab(sourceMimeType))
  }

  useEffect(() => {
    if (!!searchQuery && markdownRef.current?.view) {
      openSearchPanel(markdownRef.current?.view);
      markdownRef.current?.view?.dispatch({
        effects: [
          setSearchQuery.of(new SearchQuery({ search: searchQuery }))
        ]
      });
    }
  }, [searchQuery]);

  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <Tabs className="flex-shrink-0">
        {tabs}
      </Tabs>
    </div>
  )
}
