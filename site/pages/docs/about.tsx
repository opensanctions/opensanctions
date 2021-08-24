import { InferGetStaticPropsType } from 'next'

import Layout from '../../components/Layout'
import Content from '../../components/Content'
import { getStaticContentProps } from '../../lib/content'

export default function About({ content }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <Layout.Content content={content}>
      <Content.Page content={content} />
    </Layout.Content >
  )
}

export const getStaticProps = getStaticContentProps('about')
