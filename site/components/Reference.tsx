import { Property, PropertyType, Schema } from "@alephdata/followthemoney"
import Link from "next/link";
import Table from 'react-bootstrap/Table';
import { wordList } from "../lib/util";
import { Spacer } from "./util";


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
      {!!values.length && (
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
      )}
    </>
  )
}

type PropTypeLinkProps = {
  prop: Property
}

function PropTypeLink({ prop }: PropTypeLinkProps) {
  if (prop.hasRange) {
    return <code><Link href={`#schema.${prop.getRange().name}`}>{prop.getRange().name}</Link></code>
  }
  if (['country', 'topic', 'date'].indexOf(prop.type.name) !== -1) {
    return <code><Link href={`#type.${prop.type.name}`}>{prop.type.name}</Link></code>
  }
  return <code>{prop.type.name}</code>;
}


type SchemaReferenceProps = {
  schema: Schema
  schemata: Array<Schema>
}

export function SchemaReference({ schema, schemata }: SchemaReferenceProps) {
  const allProperties = Array.from(schema.getProperties().values())
  const properties = allProperties
    .filter(prop => !prop.hidden)
    .filter(prop => !prop.hasRange || -1 !== schemata.indexOf(prop.getRange()))
  const parents = schema.getParents()
    .map(s => <Link href={`#schema.${s.name}`}>{s.name}</Link>)
  const children = schema.getChildren()
    .filter(s => schemata.indexOf(s) !== -1)
    .map(s => <Link href={`#schema.${s.name}`}>{s.name}</Link>)
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
            <td>{wordList(parents, <Spacer />)}</td>
          </tr>
          {!!children.length && (
            <tr>
              <th>Sub-types</th>
              <td>{wordList(children, <Spacer />)}</td>
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
                {prop.schema === schema && <a id={`prop.${prop.qname}`} />}
                <code><span className="text-muted">{prop.schema.name}:</span>{prop.name}</code>
              </td>
              <td><PropTypeLink prop={prop} /></td>
              <td>{prop.label}</td>
              <td>
                {!!prop.stub && (
                  <code>
                    see{' '}
                    <Link href={`#prop.${prop.getReverse().qname}`}>{prop.getReverse().qname}</Link>
                    {' '}(inverse)
                  </code>
                )}
                {prop.description}
              </td>
            </tr>
          ))}
        </tbody>
      </Table>
    </>
  )
}