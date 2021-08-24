import { InferGetStaticPropsType } from 'next'
import Link from 'next/link'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';
import Container from 'react-bootstrap/Container';
import Badge from 'react-bootstrap/Badge';

import styles from '../styles/Home.module.scss'
import Layout from '../components/Layout'
import { getDatasets } from '../lib/api'
import { CLAIM, SPACER } from '../lib/constants'
import { getSchemaOpenSanctionsOrganization } from '../lib/schema';
import { Search } from 'react-bootstrap-icons';
import { FormattedDate, NumericBadge } from '../components/util';
import { isCollection, isSource } from '../lib/dataset';


export default function Home({ datasets }: InferGetStaticPropsType<typeof getStaticProps>) {
  const all = datasets.find((d) => d.name === 'all');
  if (all === undefined) {
    return null;
  }
  const collections = datasets.filter(isCollection)
  const sources = datasets.filter(isSource)
  return (
    <Layout.Base title={CLAIM} structured={getSchemaOpenSanctionsOrganization()}>
      <div className={styles.claimBanner}>
        <Container>
          <Row>
            <Col md={4}>
              <img src="/static/home.webp" className={styles.logo} />
            </Col>
            <Col md={8}>
              <h2 className={styles.claim}>
                {CLAIM}
              </h2>
              <p className={styles.subClaim}>
                OpenSanctions lets investigators to find leads, helps companies
                to manage risk and enables technologists to build data-driven
                products.
                {' '}<a href="/docs/about/" className={styles.claimLink}>Learn more...</a>
              </p>
              <div>
                <Button href="/datasets/" variant="light" size="lg">
                  <Search className="bsIcon" />{' '}
                  Browse datasets
                </Button>
              </div>
              <p className={styles.stats}>
                <NumericBadge value={all.target_count} className={styles.statsBadge} /> targets
                {SPACER}
                <NumericBadge value={sources.length} className={styles.statsBadge} /> data sources
                {SPACER}
                updated{' '}
                <Badge className={styles.statsBadge}>
                  <FormattedDate date={all.last_change} />
                </Badge>
              </p>
            </Col>
          </Row>
        </Container>
      </div>
      <Container>
        <Row>
          <Col md={4} className={styles.explainer}>
            <h4>People and companies that matter</h4>
            Persons of interest data works like a investigative contrast agent
            that helps analysts find evidence of corruption, money laundering
            and other criminal activity.
          </Col>
          <Col md={4} className={styles.explainer}>
            <h4>Clean data and transparent process</h4>
            We consolidate data from a <Link href="/datasets/#sources">broad range of sources</Link> and take on the
            complex task of transforming it into a clean and <Link href="/docs/reference/">well-understood
              dataset</Link>.
          </Col>
          <Col md={4} className={styles.explainer}>
            <h4>Open source code and data</h4>
            OpenSanctions makes both its database and processing tools available
            for free. It's easy to <Link href="/docs/usage">use the material</Link>,
            {' '}<Link href="/docs/contribute/">contribute to the project</Link>
            {' '}and integrate the technology.
          </Col>
        </Row>
      </Container>
    </Layout.Base>
  )
}

export const getStaticProps = async () => {
  const datasets = await getDatasets()
  return {
    props: {
      datasets
    }
  }
}
