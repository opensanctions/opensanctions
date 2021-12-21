import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'
import { getArticles, getContents } from '../lib/content';

import { getDatasets } from '../lib/data'
import writeSitemap from '../lib/sitemap';


export default function Sitemap({ }: InferGetStaticPropsType<typeof getStaticProps>) {
  return (
    <p>{'I am a banana!'}</p>
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const datasets = await getDatasets()
  const articles = await getArticles()
  const contents = await getContents()
  // const entityIds = await getCanonialEntityIds()
  writeSitemap(datasets, articles, contents)
  return {
    props: {}
  }
}
