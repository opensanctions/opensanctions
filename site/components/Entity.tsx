import Link from 'next/link'
import Card from 'react-bootstrap/Card';

import { OpenSanctionsEntity } from '../lib/types'

import styles from '../styles/Entity.module.scss'
import { Table } from 'react-bootstrap';
import { PropertyValues } from './Property';
import { Property } from '@alephdata/followthemoney';

export type EntityProps = {
  entity: OpenSanctionsEntity,
  via?: Property
}

export function EntityLink({ entity }: EntityProps) {
  if (!entity.target) {
    return <>{entity.caption}</>
  }
  return <Link href={`/entities/${entity.id}/`}>{entity.caption}</Link>
}


export function EntityCard({ entity, via }: EntityProps) {
  const props = entity.getDisplayProperties()
    .filter((p) => via === undefined || p.qname !== via.getReverse().qname);
  return (
    <Card key={entity.id} className={styles.card}>
      <Card.Header>
        <strong>{entity.schema.label}</strong>
      </Card.Header>
      <Table className={styles.cardTable}>
        <tbody>
          {props.map((prop) =>
            <tr key={prop.qname}>
              <th className={styles.cardProp}>{prop.label}</th>
              <td>
                <PropertyValues
                  prop={prop}
                  values={entity.getProperty(prop)}
                  entity={EntityLink}
                />
              </td>
            </tr>
          )}
        </tbody>
      </Table>
    </Card>
  );
}