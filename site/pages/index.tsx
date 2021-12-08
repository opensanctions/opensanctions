import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'
import Link from 'next/link'
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Button from 'react-bootstrap/Button';
import Container from 'react-bootstrap/Container';
import Badge from 'react-bootstrap/Badge';

import styles from '../styles/Home.module.scss'
import Layout from '../components/Layout'
import { getDatasets } from '../lib/data'
import { CLAIM, SUBCLAIM, SPACER, COLLECTIONS, ARTICLE_INDEX_SUMMARY } from '../lib/constants'
import { getSchemaWebSite } from '../lib/schema';
import { Search } from 'react-bootstrap-icons';
import { FormattedDate, NumericBadge } from '../components/util';
import { ICollection, isCollection, isSource } from '../lib/types';
import { getArticles } from '../lib/content';


export default function Home({ datasets, articles }: InferGetStaticPropsType<typeof getStaticProps>) {
  const structured = getSchemaWebSite()
  const all = datasets.find((d) => d.name === 'all');
  if (all === undefined) {
    return null;
  }
  const sources = datasets.filter(isSource).sort((a, b) => a.title.localeCompare(b.title))
  const oddSources = sources.filter((d, i) => i % 2 == 0)
  const evenSources = sources.filter((d, i) => i % 2 == 1)
  const allCollections = datasets.filter(isCollection)
  const collections = COLLECTIONS.map((name) => allCollections.find((c) => c.name === name)) as Array<ICollection>
  return (
    <Layout.Base title="Persons of interest database" description={SUBCLAIM} structured={structured}>
      <div className={styles.claimBanner}>
        <Container>
          <Row>
            <Col md={4}>
              <img
                src="/static/home.webp"
                width="272px"
                height="282px"
                alt="Welcome to OpenSanctions"
                className={styles.logo}
              />
            </Col>
            <Col md={8}>
              <h1 className={styles.claim}>
                {CLAIM}
              </h1>
              <p className={styles.subClaim}>
                {SUBCLAIM}
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
            Persons of interest data provides the key that helps analysts find evidence of
            corruption, money laundering and other criminal activity.
          </Col>
          <Col md={4} className={styles.explainer}>
            <h4>Clean data and transparent process</h4>
            We consolidate data from a <Link href="/datasets/#sources">broad range of sources</Link> and take on the
            complex task of transforming it into a clean and <Link href="/reference/">well-understood
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
        <Row>
          <Col md={4} className={styles.explainer}>
            <h4>Project updates</h4>
            <p>
              {ARTICLE_INDEX_SUMMARY}
            </p>
            <ul>
              {articles.map(a => (
                <li key={a.slug}>{a.date}: <Link href={a.path}>{a.title}</Link></li>
              ))}
            </ul>
            <p>
              See <Link href="/articles">all of our project updates</Link>...
            </p>
          </Col>
          <Col md={8} className={styles.explainer}>
            <h4>Collections</h4>
            <Row>
              <Col md={6}>
                <p>
                  <Link href="/docs/faq/#collections">Collections</Link> are custom datasets
                  provided by OpenSanctions that combine data from
                  many sources based on a topic.
                </p>
              </Col>
              <Col md={6}>
                <ul>
                  {collections.map(c => (
                    <li key={c.name}><Link href={c.link}>{c.title}</Link></li>
                  ))}
                </ul>
              </Col>
            </Row>
            <h4>Data sources</h4>
            <Row>
              <Col md={6}>
                <ul>
                  {oddSources.map(d => (
                    <li key={d.name}><Link href={d.link}>{d.title}</Link></li>
                  ))}
                </ul>
              </Col>
              <Col md={6}>
                <ul>
                  {evenSources.map(d => (
                    <li key={d.name}><Link href={d.link}>{d.title}</Link></li>
                  ))}
                </ul>
              </Col>
            </Row>
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const articles = await getArticles()
  return {
    props: {
      datasets: await getDatasets(),
      articles: articles.slice(0, 5)
    }
  }
}
