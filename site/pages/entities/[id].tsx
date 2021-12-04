import { GetStaticPropsContext } from 'next'
import Head from 'next/head'

import { getEntityIds } from '../../lib/data'

// import styles from '../../styles/Entity.module.scss'


type EntityScreenProps = {
  id: string
}


export default function EntityScreen({ id }: EntityScreenProps) {
  const url = `/entities/?id=${id}`;
  return (
    <>
      <Head>
        <meta http-equiv="refresh" content={`0; url=${url}`} key="refresh" />
      </Head>
      <a href={url}>{id}</a>
    </>
  );
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const id = params.id as string;
  return { props: { id } };
}

export async function getStaticPaths() {
  const ids = await getEntityIds()
  const paths = ids.map((id) => {
    return { params: { id } }
  })
  return {
    paths,
    fallback: false
  }
}
