import Link from 'next/link'
import Card from 'react-bootstrap/Card';

import { IDataset, isCollection, isSource, OpenSanctionsEntity } from '../lib/types'
import { Markdown, Numeric } from './util';
import { SPACER } from '../lib/constants';

import styles from '../styles/Entity.module.scss'
import { Badge, Table } from 'react-bootstrap';
import { PropertyValues } from './Property';

export type EntityProps = {
  entity: OpenSanctionsEntity
}

export function EntityLink({ entity }: EntityProps) {
  if (!entity.target) {
    return <>{entity.caption}</>
  }
  return <Link href={`/entities/${entity.id}/`}>{entity.caption}</Link>
}


export function EntityCard({ entity }: EntityProps) {
  return (
    <Card key={entity.id}>
      <Card.Header>
        <strong>{entity.schema.label}</strong>
      </Card.Header>
      <Table>
        <tbody>
          {entity.getProperties().map((prop) =>
            <tr key={prop.name}>
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
    </Card >
  );
}