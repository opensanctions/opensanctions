import { PropertyType, Schema } from "@alephdata/followthemoney"
import Link from "next/link";
import Table from 'react-bootstrap/Table';
import { IIndex } from "../lib/api";
import { SPACER } from "../lib/constants";
import { getSchemaChildren, getSchemaParents, wordList } from "../lib/util";


type TypeReferenceProps = {
  type: PropertyType
}

export function TypeReference({ type, children }: React.PropsWithChildren<TypeReferenceProps>) {
  const values = Array.from(type.values.entries())

  return (
    <>
      <h3><a id={`type.${type.name}`} />{type.plural}</h3>
      <p className="text-body">
        {children}
      </p>
      <Table striped bordered size="sm">
        <thead>
          <tr>
            <th style={{ width: "15%" }}>Code</th>
            <th>Label</th>
          </tr>
        </thead>
        <tbody>
          {values.map(([code, label]) => (
            <tr key={code}>
              <td><code>{code}</code></td>
              <td>{label}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </>
  )
}


type SchemaReferenceProps = {
  schema: Schema
  index: IIndex
}

export function SchemaReference({ schema, index }: SchemaReferenceProps) {
  const allProperties = Array.from(schema.getProperties().values())
  const properties = allProperties
    .filter(prop => !prop.hidden)
    .filter(prop => !prop.hasRange || -1 !== index.schemata.indexOf(prop.getRange().name))
  const parents = getSchemaParents(schema).map(s => <code>{s.name}</code>)
  const children = getSchemaChildren(schema).map(s => <code>{s.name}</code>)
  return (
    <>
      <h4><a id={`schema.${schema.name}`} /><code>{schema.name}</code> - {schema.plural}</h4>
      <p className="text-body">
        {schema.description}
      </p>
      <Table striped bordered size="sm">
        <tbody>
          <tr>
            <th>Extends</th>
            <td>{wordList(parents, SPACER)}</td>
          </tr>
          {!!children.length && (
            <tr>
              <th>Sub-types</th>
              <td>{wordList(children, SPACER)}</td>
            </tr>
          )}
        </tbody>
      </Table>
      <Table striped bordered size="sm">
        <thead>
          <tr>
            <th>Property</th>
            <th>Type</th>
            <th>Title</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {properties.map(prop => (
            <tr key={prop.name}>
              <td>
                <code><span className="text-muted">{prop.schema.name}.</span>{prop.name}</code>
              </td>
              <td><code>{prop.hasRange ? <Link href={`#schema.${prop.getRange().name}`}>{prop.getRange().name}</Link> : prop.type.name}</code></td>
              <td>{prop.label}</td>
              <td>{prop.description}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </>
  )
}