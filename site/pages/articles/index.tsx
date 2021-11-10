import { InferGetStaticPropsType } from 'next'
import Link from 'next/link';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import { getArticles } from '../../lib/content'
import { Github, Slack, Twitter } from 'react-bootstrap-icons';
import { FormattedDate, Summary } from '../../components/util';
import { ARTICLE_INDEX_SUMMARY } from '../../lib/constants';

import styles from '../../styles/Article.module.scss';


export default function ArticleIndex({ articles }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <Layout.Base title="Articles" structured={undefined}>
      <Container>
        <h1>
          What's happening at OpenSanctions?
        </h1>
        <Summary summary={ARTICLE_INDEX_SUMMARY} />
        <Row>
          <Col md={8}>
            <ul className={styles.articleList}>
              {articles.map((article) => (
                <li key={article.slug}>
                  <p className={styles.articleListTitle}>
                    <span className={styles.articleListDate}>
                      <FormattedDate date={article.date} />
                      {': '}
                    </span>
                    <Link href={article.path}>{article.title}</Link>
                  </p>
                  <p className={styles.articleListSummary}>

                    {article.summary}
                  </p>
                </li>
              ))}
            </ul>
          </Col>
          <Col md={4}>
            <strong>More ways to keep in touch</strong>
            <ul>
              <li>
                <Link href="https://twitter.com/open_sanctions"><Twitter /></Link>
                {' '}
                <Link href="https://twitter.com/open_sanctions">Twitter</Link>
              </li>
              <li>
                <Link href="https://bit.ly/osa-slack"><Slack /></Link>
                {' '}
                <Link href="https://bit.ly/osa-slack">Slack chat</Link>
              </li>
              <li>
                <Link href="https://github.com/pudo/opensanctions"><Github /></Link>
                {' '}
                <Link href="https://github.com/pudo/opensanctions">Github code</Link>
              </li>
            </ul>
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async () => {
  return {
    props: {
      articles: await getArticles()
    }
  }
}
