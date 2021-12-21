import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'

import Layout from '../../components/Layout'
import Content from '../../components/Content'
import { getContentBySlug, getContents, getStaticContentProps } from '../../lib/content'

export default function DocsContent({ content }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <Layout.Content content={content}>
      <Content.Page content={content} />
    </Layout.Content >
  )
}


export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const content = await getContentBySlug(params.slug as string)
  if (content === undefined) {
    return { redirect: { destination: '/', permanent: false } };
  }
  return { props: { content } }
}

export async function getStaticPaths() {
  const contents = await getContents()
  const paths = contents
    .filter((c) => c.path === `/docs/${c.slug}/`)
    .map((c) => {
      return { params: { slug: c.slug } }
    })
  return {
    paths,
    fallback: false
  }
}

