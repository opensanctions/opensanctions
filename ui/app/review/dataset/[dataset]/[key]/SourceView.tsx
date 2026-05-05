"use client";

import React, { useRef, useEffect } from 'react';
import { markdown } from "@codemirror/lang-markdown";
import CodeMirror from "@uiw/react-codemirror";
import { yaml } from "@codemirror/lang-yaml";
import { EditorView } from "@codemirror/view";
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import TurndownService from 'turndown';
import { stringify as stringifyToYaml } from 'yaml';

import { ReviewEntity } from '@/lib/db';
import { createHighlighter } from '@/lib/codemirror';
import styles from "@/styles/Review.module.scss";

type SourceViewProps = {
  sourceValue: string,
  sourceMimeType: string,
  sourceLabel: string,
  sourceSearchQuery: string,
  relatedEntities: ReviewEntity[],
  onTextSelect?: (text: string) => void
};

function makeCodeMirror(
  title: string,
  value: string,
  extensions: any[],
  searchQuery: string,
  onTextSelect?: (text: string) => void
) {
  const highlighter = createHighlighter(searchQuery);

  const selectionHandler = onTextSelect ? EditorView.domEventHandlers({
    mouseup: (event, view) => {
      const selection = view.state.selection.main;
      if (selection.from !== selection.to) {
        const selectedText = view.state.doc.sliceString(selection.from, selection.to).trim();
        if (selectedText.length > 0) {
          onTextSelect(selectedText);
        }
      }
    }
  }) : null;

  return <CodeMirror
    value={value}
    height="100%"
    width="100%"
    editable={false}
    style={{ height: '100%', width: '100%' }}
    extensions={[
      EditorView.lineWrapping,
      ...extensions,
      ...(highlighter ? [highlighter] : []),
      ...(selectionHandler ? [selectionHandler] : [])
    ]}
    title={title}
    basicSetup={{
      searchKeymap: true,
    }}
  />
}

function SourceView({ sourceValue, sourceMimeType, sourceLabel, sourceSearchQuery, relatedEntities, onTextSelect }: SourceViewProps) {

  const handleRenderedTextSelection = () => {
    if (!onTextSelect) return;

    const selection = window.getSelection();
    const selectedText = selection?.toString().trim();

    if (selectedText && selectedText.length > 0) {
      onTextSelect(selectedText);
    }
  };

  const tabs: React.ReactNode[] = [];
  const tab = (title: string, content: React.ReactNode | null = null) => {
    return <Tab key={title} eventKey={title} title={title} >
      {content ? content : makeCodeMirror(title, sourceValue, [], sourceSearchQuery, onTextSelect)}
    </Tab>
  }

  const htmlRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (htmlRef.current && sourceMimeType === 'text/html') {
      let highlighted: string;
      if (sourceSearchQuery) {
        const regexp = new RegExp(RegExp.escape(sourceSearchQuery), 'gi');
        highlighted = sourceValue.replace(regexp, (match) => `<mark>${match}</mark>`);
      } else {
        highlighted = sourceValue;
      }
      htmlRef.current.setHTML(highlighted);
    }
  }, [sourceValue, sourceSearchQuery, sourceMimeType]);

  if (sourceMimeType === 'text/html') {
    tabs.push(tab("As web page",
      <div
        ref={htmlRef}
        suppressHydrationWarning
        style={{ height: '100%', width: '100%', overflow: 'auto', backgroundColor: '#fff', padding: '10px' }}
        onMouseUp={handleRenderedTextSelection} />
    ))

    const turndownService = new TurndownService();
    tabs.push(
      tab(
        "As Markdown",
        makeCodeMirror("As Markdown", turndownService.turndown(sourceValue), [markdown()], sourceSearchQuery, onTextSelect)
      )
    );

    tabs.push(tab("Original HTML"))
  } else if (sourceMimeType === 'application/json') {
    const valueYaml = stringifyToYaml(JSON.parse(sourceValue), null, { indent: 2, lineWidth: 0 });
    tabs.push(
      tab(
        "Original JSON as YAML",
        makeCodeMirror("Original JSON as YAML", valueYaml, [yaml()], sourceSearchQuery, onTextSelect)
      )
    );
  } else if (sourceMimeType === 'text/plain') {
    tabs.push(tab("Original Plain Text"))
  } else if (sourceMimeType === 'image/png') {
    tabs.push(tab("Original PNG", <img src={sourceValue} alt={sourceLabel} />))
  } else {
    tabs.push(tab(sourceMimeType))
  }

  return (
    <div className="flex-grow-1 d-flex flex-column source-view" style={{ height: '100%' }}>
      <div className="d-flex flex-column" style={{ flex: 1, minHeight: 0 }}>
        <Tabs>
          {tabs}
        </Tabs>
      </div>
      {relatedEntities.length > 0 && (
        <div className="d-flex flex-column" style={{ flex: 1, minHeight: 0 }}>
          <h2 className="h6 mt-2 mb-1">Related entities</h2>
          <Tabs className={styles.entityTabs}>
            {relatedEntities.map((entity) => (
              <Tab key={entity.entity_id} eventKey={entity.entity_id} title={entity.entity_id}>
                <iframe
                  src={`https://www.opensanctions.org/entities/preview/${entity.entity_id}/`}
                  className={styles.entityPreview}
                  title={entity.entity_id}
                />
              </Tab>
            ))}
          </Tabs>
        </div>
      )}
    </div>
  )
}

export default React.memo(SourceView);
