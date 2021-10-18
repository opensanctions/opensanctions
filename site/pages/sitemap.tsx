import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'

import { getCanonialEntityIds, getDatasets } from '../lib/data'
import writeSitemap from '../lib/sitemap';


export default function Home({ }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <p>I'm a banana!</p>
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const datasets = await getDatasets()
  const entityIds = await getCanonialEntityIds()
  writeSitemap(datasets, entityIds)
  return {
    props: {}
  }
}
