import { useRouter } from 'next/router'
import Head from 'next/head'
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next';

import { BASE_URL } from '../../lib/constants';


export default function EntityPreview({ }: InferGetStaticPropsType<typeof getStaticProps>) {
  const router = useRouter();
  const entityId = router.query.id;
  const url = `${BASE_URL}/entities/${entityId}/`
  return (
    <>
      <Head>
        <title>Redirect to {entityId}</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta http-equiv="refresh" content={`0; url=${url}`} />
      </Head>
      <p>
        See: <a href={url}>{entityId}</a>
      </p>
    </>
  );
}


export const getStaticProps = async (context: GetStaticPropsContext) => {
  return {
    props: {}
  };
}
