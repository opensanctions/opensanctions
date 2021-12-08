import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'
import { getArticles } from '../lib/content';

import { getCanonialEntityIds, getDatasets } from '../lib/data'
import writeSitemap from '../lib/sitemap';


export default function Sitemap({ }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <p>I'm a banana!</p>
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const datasets = await getDatasets()
  const articles = await getArticles()
  const entityIds = await getCanonialEntityIds()
  writeSitemap(datasets, articles, entityIds)
  return {
    props: {}
  }
}
