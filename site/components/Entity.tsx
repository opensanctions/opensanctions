import Head from 'next/head';
import Link from 'next/link'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Card from 'react-bootstrap/Card';
import Table from 'react-bootstrap/Table';
import { Property } from '@alephdata/followthemoney';

import { IDataset, IOpenSanctionsEntity, OpenSanctionsEntity } from '../lib/types'
import { PropertyValues } from './Property';
import { Summary } from './util';
import Dataset from './Dataset';
import { BASE_URL } from '../lib/constants';

import styles from '../styles/Entity.module.scss'


export type EntityProps = {
  entity: OpenSanctionsEntity,
  showEmpty?: boolean
  via?: Property
}

export function EntityLink({ entity }: EntityProps) {
  return <Link href={`/entities/?id=${entity.id}`}>{entity.caption}</Link>
}


export function EntityCard({ entity, via, showEmpty = false }: EntityProps) {
  const props = entity.getDisplayProperties()
    .filter((p) => via === undefined || p.qname !== via.getReverse().qname)
    .filter((p) => showEmpty || entity.getProperty(p).length > 0)
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

export type EntitySidebarProps = {
  entity: OpenSanctionsEntity,
}

export function EntitySidebar({ entity }: EntityProps) {
  const properties = entity.getDisplayProperties();
  const sidebarProperties = properties.filter((p) => p.type.name !== 'entity' && p.name !== 'notes');
  return (
    <>
      <p>
        <strong>Type</strong><br />
        <span>{entity.schema.label}</span>
      </p>
      {sidebarProperties.map((prop) =>
        <p key={prop.name}>
          <strong>{prop.label}</strong><br />
          <span><PropertyValues prop={prop} values={entity.getProperty(prop)} /></span>
        </p>
      )}
    </>
  );
}

export type EntityDisplayProps = {
  entity: OpenSanctionsEntity,
  datasets: Array<IDataset>
}

export function EntityDisplay({ entity, datasets }: EntityDisplayProps) {
  const properties = entity.getDisplayProperties();
  const entityProperties = properties.filter((p) => p.type.name === 'entity');
  return (
    <Row>
      <Col md={3}>
        <EntitySidebar entity={entity} />
      </Col>
      <Col md={9}>
        {entity.hasProperty('notes') && (
          <div className={styles.entityPageSection}>
            {/* <h2>Notes</h2> */}
            {entity.getProperty('notes').map((v, idx) => (
              <Summary key={idx} summary={v as string} />
            ))}
          </div>
        )}
        {entityProperties.map((prop) =>
          <div className={styles.entityPageSection} key={prop.qname}>
            <h2>{prop.getRange().plural}</h2>
            {entity.getProperty(prop).map((value) =>
              <EntityCard
                key={(value as OpenSanctionsEntity).id}
                entity={value as OpenSanctionsEntity}
                via={prop}
              />
            )}
          </div>
        )}
        <div className={styles.entityPageSection}>
          <h2>Data sources</h2>
          <Row>
            {datasets.map((d) => (
              <Col md={4} key={d.name}>
                <Dataset.Card dataset={d} />
              </Col>
            ))}
          </Row>
        </div>
      </Col>
    </Row>
  );
}


type EntityRedirectProps = {
  entity: IOpenSanctionsEntity
}

export function EntityRedirect({ entity }: EntityRedirectProps) {
  const url = `${BASE_URL}/entities/?id=${entity.id}`
  return (
    <>
      <Head>
        <title>Redirect to {entity.caption}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta http-equiv="refresh" content={`0; url=${url}`} />
      </Head>
      <p>
        See: <a href={url}>{entity.caption}</a>
      </p>
    </>
  );
}
