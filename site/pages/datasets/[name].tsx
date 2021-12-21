import { useState, useEffect } from 'react';
import { GetStaticPropsContext } from 'next'
import Link from 'next/link';
import { useRouter } from 'next/router';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Card from 'react-bootstrap/Card';
import Table from 'react-bootstrap/Table';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import InputGroup from 'react-bootstrap/InputGroup';
import Alert from 'react-bootstrap/Alert';
import Tooltip from 'react-bootstrap/Tooltip';
import ListGroup from 'react-bootstrap/ListGroup';
import Container from 'react-bootstrap/Container';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import { FileEarmarkArrowDownFill } from 'react-bootstrap-icons';

import Layout from '../../components/Layout'
import Dataset from '../../components/Dataset'
import { getDatasets, getDatasetByName, getDatasetIssues, getDatasetDetails } from '../../lib/data'
import { IDataset, IIssue, ICollection, ISource, isCollection, isSource, LEVEL_ERROR, LEVEL_WARNING, IDatasetDetails } from '../../lib/types'
import { Summary, FileSize, NumericBadge, JSONLink, HelpLink, Markdown } from '../../components/util'
import DatasetMetadataTable from '../../components/DatasetMetadataTable'
import { getSchemaDataset } from '../../lib/schema';
import { IssuesList } from '../../components/Issue';
import { SPACER } from '../../lib/constants';

import styles from '../../styles/Dataset.module.scss'
import { LicenseInfo } from '../../components/Policy';


type DatasetScreenProps = {
  dataset: IDataset
  details: IDatasetDetails
  issues: Array<IIssue>
  sources?: Array<ISource>
  collections?: Array<ICollection>
}

export default function DatasetScreen({ dataset, details, issues, sources, collections }: DatasetScreenProps) {
  const router = useRouter();
  const [view, setView] = useState('description');
  const errors = issues.filter((i) => i.level === LEVEL_ERROR)
  const warnings = issues.filter((i) => i.level === LEVEL_WARNING)
  const structured = getSchemaDataset(dataset, details)

  useEffect(() => {
    router.events.on("routeChangeComplete", async () => setView('description'))
  })

  return (
    <Layout.Base title={dataset.title} description={dataset.summary} structured={structured} navSearch={false}>
      <Container>
        <JSONLink href={dataset.index_url} />
        <h1>
          <Dataset.Icon dataset={dataset} size="30px" /> {dataset.title}
        </h1>
        <Row>
          <Col sm={9}>
            <Summary summary={dataset.summary} />
            <Form className="d-flex" action="/search/">
              <input type="hidden" name="scope" value={dataset.name} />
              <InputGroup className={styles.searchBox} size="lg">
                <Form.Control
                  type="search"
                  name="q"
                  autoFocus={true}
                  placeholder={`Search in ${dataset.title}...`}
                  aria-label="Search"
                />
                <Button variant="secondary" type="submit">
                  Search
                </Button>
              </InputGroup>
            </Form>


            <DatasetMetadataTable dataset={dataset} details={details} collections={collections} />
            <Tabs activeKey={view} defaultActiveKey="description" onSelect={(k) => setView(k || 'description')}>
              <Tab eventKey="description" title="Description" className={styles.viewTab}>
                <Markdown markdown={details.description} />
                {isCollection(dataset) && (
                  <Alert variant="warning">
                    This collection contains entities from multiple data sources. <Link href="/articles/2021-11-11-deduplication/">We have made sigificant progress</Link> on
                    the de-duplication of entities across sources, but it is not yet complete - so individual companies or people may be
                    contained more than once.
                  </Alert>
                )}
              </Tab>
              {isSource(dataset) && !!errors.length && (
                <Tab eventKey="errors" title={<>{'Errors'} <NumericBadge value={errors.length} bg="danger" /></>} className={styles.viewTab}>
                  <IssuesList issues={errors} showDataset={false} />
                </Tab>
              )}
              {isSource(dataset) && !!warnings.length && (
                <Tab eventKey="warnings" title={<>{'Warnings'} <NumericBadge value={warnings.length} bg="warning" /></>} className={styles.viewTab}>
                  <IssuesList issues={warnings} showDataset={false} />
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
              {!!details.targets.countries.length && (
                <Tab eventKey="profile" title={<>{'Geographic coverage'} <NumericBadge value={details.targets.countries.length} /></>} className={styles.viewTab}>
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
                        {details.targets.countries.map(c =>
                          <tr key={c.code}>
                            <td><code>{c.code}</code></td>
                            <td>
                              <a href={`/search/?scope=${dataset.name}&countries=${c.code}`}>
                                {c.label}
                              </a>
                            </td>
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
              <Card.Header><strong>Downloads</strong></Card.Header>
              <ListGroup variant="flush">
                {details.resources.map((resource) =>
                  <ListGroup.Item key={resource.path}>
                    <Button
                      variant="secondary"
                      rel="nofollow"
                      // @ts-expect-error
                      download={true}
                      href={resource.url}
                    >
                      <FileEarmarkArrowDownFill className="bsIcon" />
                      {' '}
                      {resource.title}
                    </Button>
                    <div>
                      <FileSize size={resource.size} />
                      {SPACER}
                      <OverlayTrigger placement="bottom" overlay={<Tooltip>{resource.mime_type_label}</Tooltip>}>
                        <code>{resource.mime_type}</code>
                      </OverlayTrigger>
                      <HelpLink href={`/docs/usage/#${resource.path}`} />
                    </div>
                  </ListGroup.Item>
                )}
              </ListGroup>
              <Card.Footer className="text-muted">
                Help: <Link href="/docs/usage">Using the data</Link>
                {SPACER} <Link href="/reference/">reference</Link>
              </Card.Footer>
            </Card>
            <LicenseInfo />
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const dataset = await getDatasetByName(params.name as string)
  const details = await getDatasetDetails(params.name as string)
  if (dataset === undefined || details === undefined) {
    return { redirect: { destination: '/', permanent: false } };
  }
  const issues = await getDatasetIssues(dataset)
  const props: DatasetScreenProps = { dataset, issues, details }
  if (isCollection(dataset)) {
    const sources = await Promise.all(dataset.sources.map((name) => getDatasetByName(name)))
    const visibleSources = sources.filter((s) => s !== undefined && !s.hidden)
    props.sources = visibleSources as Array<ISource>
  }
  if (isSource(dataset)) {
    const collections = await Promise.all(dataset.collections.map((name) => getDatasetByName(name)))
    const visibleCollections = collections.filter((c) => isCollection(c) && !c.hidden)
    props.collections = visibleCollections as Array<ICollection>
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
