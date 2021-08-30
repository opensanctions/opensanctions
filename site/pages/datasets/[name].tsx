import { useState } from 'react';
import { GetStaticPropsContext } from 'next'
import Link from 'next/link';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Card from 'react-bootstrap/Card';
import Table from 'react-bootstrap/Table';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import Tooltip from 'react-bootstrap/Tooltip';
import ListGroup from 'react-bootstrap/ListGroup';
import Container from 'react-bootstrap/Container';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import { FileEarmarkArrowDownFill } from 'react-bootstrap-icons';

import Layout from '../../components/Layout'
import Dataset from '../../components/Dataset'
import { getDatasets, getDatasetByName, getDatasetIssues } from '../../lib/api'
import { IDataset, ICollection, ISource, isCollection, isSource, IIssueIndex, LEVEL_ERROR, LEVEL_WARNING } from '../../lib/dataset'
import { Summary, FileSize, NumericBadge, JSONLink, HelpLink } from '../../components/util'
import DatasetMetadataTable from '../../components/DatasetMetadataTable'
import { getSchemaDataset } from '../../lib/schema';
import IssuesTable from '../../components/Issue';
import { SPACER } from '../../lib/constants';

import styles from '../../styles/Dataset.module.scss'


type DatasetScreenProps = {
  dataset?: IDataset
  structured: any
  issues: IIssueIndex
  sources?: Array<ISource>
  collections?: Array<ICollection>
}

export default function DatasetScreen({ dataset, structured, issues, sources, collections }: DatasetScreenProps) {
  if (dataset === undefined) {
    return null;
  }
  const [view, setView] = useState('description');
  const errors = issues.issues.filter((i) => i.level === LEVEL_ERROR)
  const warnings = issues.issues.filter((i) => i.level === LEVEL_WARNING)

  return (
    <Layout.Base title={dataset.title} description={dataset.summary} structured={structured}>
      <Container>
        <h1>
          <Dataset.Icon dataset={dataset} size="30px" /> {dataset.title}
          <JSONLink href={dataset.index_url} />
        </h1>
        <Row>
          <Col sm={9}>
            <Summary summary={dataset.summary} />
            <DatasetMetadataTable dataset={dataset} collections={collections} />
            <Tabs activeKey={view} defaultActiveKey="description" onSelect={(k) => setView(k || 'description')}>
              <Tab eventKey="description" title="Description" className={styles.viewTab}>
                <Dataset.Description dataset={dataset} />
              </Tab>
              {isSource(dataset) && !!errors.length && (
                <Tab eventKey="errors" title={<>{'Errors'} <NumericBadge value={errors.length} bg="danger" /></>} className={styles.viewTab}>
                  <IssuesTable issues={errors} />
                </Tab>
              )}
              {isSource(dataset) && !!warnings.length && (
                <Tab eventKey="warnings" title={<>{'Warnings'} <NumericBadge value={warnings.length} bg="warning" /></>} className={styles.viewTab}>
                  <IssuesTable issues={warnings} />
                </Tab>
              )}
              {isCollection(dataset) && sources?.length && (
                <Tab eventKey="sources" title={<>{'Data sources'} <NumericBadge value={sources.length} /></>} className={styles.viewTab}>
                  <Row>
                    {sources.map((d) => (
                      <Col md={4} key={d.name}>
                        <Dataset.Card dataset={d} />
                      </Col>
                    ))}
                  </Row>
                </Tab>
              )}
              {!!dataset.targets.countries.length && (
                <Tab eventKey="profile" title={<>{'Geographic coverage'} <NumericBadge value={dataset.targets.countries.length} /></>} className={styles.viewTab}>
                  <>
                    <p>
                      {dataset.title} includes target entities in the following countries.
                      {' '}<Link href="/reference/#type.country">Read about countries...</Link>
                    </p>
                    <Table>
                      <thead>
                        <tr>
                          <th style={{ width: "10%" }}>Code</th>
                          <th>Country</th>
                          <th className="numeric">Targets</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dataset.targets.countries.map(c =>
                          <tr key={c.code}>
                            <td><code>{c.code}</code></td>
                            <td>{c.label}</td>
                            <td className="numeric">{c.count}</td>
                          </tr>
                        )}
                      </tbody>
                    </Table>
                  </>
                </Tab>
              )}
            </Tabs>
          </Col>
          <Col sm={3}>
            <Card>
              <Card.Header>Downloads</Card.Header>
              <ListGroup variant="flush">
                {dataset.resources.map((resource) =>
                  <ListGroup.Item key={resource.path}>
                    <a href={resource.url} download={true}>
                      <FileEarmarkArrowDownFill className="bsIcon" />
                    </a>
                    {' '}
                    <a href={resource.url} download={true}>
                      {resource.title}
                    </a>
                    {' '}
                    <HelpLink href={`/docs/usage/#${resource.path}`} />
                    <div>
                      <FileSize size={resource.size} />
                      {SPACER}
                      <OverlayTrigger placement="bottom" overlay={<Tooltip>{resource.mime_type_label}</Tooltip>}>
                        <code>{resource.mime_type}</code>
                      </OverlayTrigger>
                    </div>
                  </ListGroup.Item>
                )}
              </ListGroup>
              <Card.Footer className="text-muted">
                Help: <Link href="/docs/usage">Using the data</Link>
                {SPACER} <Link href="/reference/">reference</Link>
              </Card.Footer>
            </Card>
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const dataset = await getDatasetByName(params.name as string)
  const structured = await getSchemaDataset(params.name as string)
  const issues = await getDatasetIssues(dataset)
  const props: DatasetScreenProps = { dataset, structured, issues }
  if (isCollection(dataset)) {
    const sources = await Promise.all(dataset.sources.map((name) => getDatasetByName(name)))
    props.sources = sources as Array<ISource>
  }
  if (isSource(dataset)) {
    const collections = await Promise.all(dataset.collections.map((name) => getDatasetByName(name)))
    props.collections = collections.filter((d) => isCollection(d) && d.name !== 'all') as Array<ICollection>
  }
  return { props }
}

export async function getStaticPaths() {
  const datasets = await getDatasets()
  const paths = datasets.map((dataset) => {
    return { params: { name: dataset.name } }
  })
  return {
    paths,
    fallback: false
  }
}
