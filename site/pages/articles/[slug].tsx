import { GetStaticPropsContext } from 'next'
import Link from 'next/link';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Card from 'react-bootstrap/Card';
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import { IArticle } from '../../lib/types'
import { Summary } from '../../components/util'
import { getArticleBySlug, getArticles } from '../../lib/content';
import Content from '../../components/Content';
import Article from '../../components/Article';

import styles from '../../styles/Article.module.scss';
import { getSchemaArticle } from '../../lib/schema';

type ArticleScreenProps = {
  article: IArticle
}

export default function ArticleScreen({ article }: ArticleScreenProps) {
  const structured = getSchemaArticle(article)
  return (
    <Layout.Base title={article.title} description={article.summary} structured={structured}>
      <Container>
        <h1>{article.title}</h1>
        <Row>
          <Col md={8}>
            <Summary summary={article.summary} />
            <Content.Body content={article} />
            <Card>
              <Card.Body>
                <i>
                  <strong>Like what we're writing about?</strong> Keep the conversation going! You
                  can <Link href="https://twitter.com/open_sanctions">follow us on Twitter</Link> or
                  join the <Link href="https://twitter.com/open_sanctions">Slack chat</Link> to
                  bring in your own ideas and questions. Or, check out the <Link href="/docs/">project
                    documentation</Link> to learn more about OpenSanctions.
                </i>
              </Card.Body>
            </Card>
          </Col>
          <Col md={4} className="d-print-none">
            <Article.Sidebar article={article} />
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const article = await getArticleBySlug(params.slug as string)
  if (article === undefined) {
    return { redirect: { destination: '/', permanent: false } };
  }
  return { props: { article } }
}

export async function getStaticPaths() {
  const articles = await getArticles()
  const paths = articles.map((article) => {
    return { params: { slug: article.slug } }
  })
  return {
    paths,
    fallback: false
  }
}
