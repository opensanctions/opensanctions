import { InferGetStaticPropsType } from 'next'
import Link from 'next/link'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import Dataset from '../../components/Dataset'
import { INDEX_URL } from '../../lib/constants';
import { getDatasets } from '../../lib/api'
import { getSchemaDataCatalog } from '../../lib/schema'
import { ICollection, isCollection, isSource } from '../../lib/types';
import { JSONLink } from '../../components/util';


// fake up a semantic ordering of collections
const COLLECTIONS = ['sanctions', 'default', 'peps', 'crime'];

export default function DatasetIndex({ datasets, structured }: InferGetStaticPropsType<typeof getStaticProps>) {
  const allCollections = datasets.filter(isCollection)
  const collections = COLLECTIONS.map(n => allCollections.find(c => c.name == n)) as Array<ICollection>
  const sources = datasets.filter(isSource)
  return (
    <Layout.Base title="Datasets" structured={structured}>
      <Container>
        <h1>
          <a id="collections" />
          Collections
          <JSONLink href={INDEX_URL} />
        </h1>
        <Row>
          <Col md={3}>
            <p>
              <strong>Collections</strong> are custom datasets
              provided by OpenSanctions that combine data from
              many sources based on a topic.
              {' '}<Link href="/docs/faq/#collections">Learn more...</Link>
            </p>
          </Col>
          <Col md={9}>
            <Row>
              {collections.map((d) => (
                <Col sm={6} key={d.name}>
                  <Dataset.Card dataset={d} />
                </Col>
              ))}
            </Row>
          </Col>
        </Row>
        <hr />
        <h1>
          <a id="sources" />
          Data sources
        </h1>
        <Row>
          <Col md={3}>
            <p>
              <strong>Data sources</strong> collect targeted entities from a
              particular origin. Many sources are published by government authorities
              or international organisations.
            </p>
            <p>
              Can't find a data source you are looking for? Check the page
              on <Link href="/docs/contribute/">contributing a data source</Link> to
              learn about planned additions and how you can help.
            </p>
          </Col>
          <Col md={9}>
            <Row>
              {sources.map((d) => (
                <Col sm={4} key={d.name}>
                  <Dataset.Card dataset={d} />
                </Col>
              ))}
            </Row>
          </Col>
        </Row>
      </Container>
    </Layout.Base>
  )
}

export const getStaticProps = async () => {
  return {
    props: {
      datasets: await getDatasets(),
      structured: await getSchemaDataCatalog()
    }
  }
}
