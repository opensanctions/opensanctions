"use client";

import { markdown } from "@codemirror/lang-markdown";
import { openSearchPanel, setSearchQuery, SearchQuery } from "@codemirror/search";
import CodeMirror, { ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { EditorView } from "codemirror";
import { useEffect, useRef } from "react";
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import TurndownService from 'turndown';

import { ReviewEntity } from '@/lib/db';
import styles from "@/styles/Review.module.scss";

type SourceViewProps = {
  sourceValue: string,
  sourceMimeType: string,
  sourceLabel: string,
  searchQuery: string,
  relatedEntities: ReviewEntity[]
};

function searchInCodemirror(ref: React.RefObject<ReactCodeMirrorRef | null>, searchQuery: string) {
  if (ref.current?.view) {
    openSearchPanel(ref.current?.view);
    ref.current?.view?.dispatch({
      effects: [
        setSearchQuery.of(new SearchQuery({ search: searchQuery }))
      ]
    });
  }
}

export default function SourceView({ sourceValue, sourceMimeType, sourceLabel, searchQuery, relatedEntities }: SourceViewProps) {
  const rawValueRef = useRef<ReactCodeMirrorRef>(null);
  const markdownRef = useRef<ReactCodeMirrorRef>(null);

  const tabs: React.ReactNode[] = [];
  const tab = (title: string, content: React.ReactNode | null = null) => {
    return <Tab key={title} eventKey={title} title={title} >
      {content ? content : <CodeMirror
        ref={rawValueRef}
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
    if (!!searchQuery && rawValueRef.current)
      searchInCodemirror(rawValueRef, searchQuery);
    if (!!searchQuery && markdownRef.current)
      searchInCodemirror(markdownRef, searchQuery);
  }, [searchQuery]);

  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <Tabs className="flex-shrink-0">
        {tabs}
      </Tabs>
      {relatedEntities.length > 0 ? (
        <div>
          <h4 className="h6 pt-2">Related entities</h4>
          <ul className={styles.relatedEntities}>
            {relatedEntities.map((entity) => (
              <li key={entity.entity_id}>
                <a
                  href={`https://opensanctions.org/entities/${entity.entity_id}/`}
                  target="_blank"
                >
                  {entity.entity_id}
                </a>
              </li>
            ))}
          </ul>
        </div>
      ) : <p className="m-2">No entities linked to this review.</p>}
    </div>
  )
}
