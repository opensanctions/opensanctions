import React, { ReactElement } from 'react'
import unified from 'unified'
import remarkParse from 'remark-parse'
import remarkRehype from 'remark-rehype'
import rehypeRaw from 'rehype-raw'
import rehypeStringify from 'rehype-stringify'
import { Schema } from '@alephdata/followthemoney'


export function markdownToHtml(markdown: string): string {
  const result = unified()
    .use(remarkParse)
    .use(remarkRehype, { allowDangerousHtml: true })
    .use(rehypeRaw)
    .use(rehypeStringify)
    .processSync(markdown)
  return result.contents as string;
}

/*
 * https://stackoverflow.com/questions/23618744/rendering-comma-separated-list-of-links
 */
export function wordList(arr: Array<any>, sep: string): ReactElement {
  if (arr.length === 0) {
    return <></>;
  }

  return arr.slice(1)
    .reduce((xs, x, i) => xs.concat([
      <span key={`${i}_sep`} className="separator">{sep}</span>,
      <span key={i}>{x}</span>
    ]), [<span key={arr[0]}>{arr[0]}</span>])
}


export function getSchemaParents(schema: Schema): Array<Schema> {
  const parents = new Map<string, Schema>()
  for (const ext of schema.getExtends()) {
    parents.set(ext.name, ext)
    for (const parent of getSchemaParents(ext)) {
      parents.set(parent.name, parent)
    }
  }
  return Array.from(parents.values())
}

export function getSchemaChildren(schema: Schema): Array<Schema> {
  const children = new Array<Schema>()
  for (const ms of schema.model.getSchemata()) {
    const parents = getSchemaParents(ms)
    if (parents.indexOf(schema) !== -1 && children.indexOf(schema) === -1) {
      children.push(ms)
    }
  }
  return children;
}

export function getAllParents(schemata: Array<Schema>): Array<Schema> {
  const parents = Array.from(schemata)
  for (const schema of schemata) {
    for (const parent of getSchemaParents(schema)) {
      if (parents.indexOf(parent) === -1) {
        parents.push(parent)
      }
    }
  }
  return parents;
}