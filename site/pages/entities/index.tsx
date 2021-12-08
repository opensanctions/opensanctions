import { Model } from '@alephdata/followthemoney';
import { useRouter } from 'next/router'
import Head from 'next/head'
import useSWR from 'swr';
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import { ISource, isSource, OpenSanctionsEntity } from '../../lib/types';
import { fetchIndex, getDatasets } from '../../lib/data';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next';
import { swrFetcher } from '../../lib/util';
import { API_URL } from '../../lib/constants';
import { JSONLink, SectionSpinner } from '../../components/util';
import { getSchemaEntityPage } from '../../lib/schema';
import { EntityDisplay, EntityRedirect } from '../../components/Entity';

// import styles from '../styles/Search.module.scss'


export default function EntityPreview({ modelData, datasets }: InferGetStaticPropsType<typeof getStaticProps>) {
  const model = new Model(modelData);
  const router = useRouter();
  const entityId = router.query.id;
  const apiUrl = entityId ? `${API_URL}/entities/${entityId}` : undefined;
  const { data, error } = useSWR(apiUrl, swrFetcher);
  if (error) {
    return (
      <Layout.Base title="Failed to load">
        <Container>
          <h1 className="errorTitle">
            Could not load entity
          </h1>
        </Container>
      </Layout.Base >
    );
  }
  if (!data || !apiUrl) {
    return (
      <Layout.Base title="Loading...">
        <Container>
          <SectionSpinner />
        </Container>
      </Layout.Base >
    );
  }
  if (data.id !== undefined && data.id !== entityId) {
    return <EntityRedirect entity={data} />
  }
  const entity = OpenSanctionsEntity.fromData(model, data)
  const structured = getSchemaEntityPage(entity, datasets)
  const sources = entity.datasets
    .map((name) => datasets.find((d) => d.name === name))
    .filter((d) => d !== undefined)
    .filter((d) => isSource(d))

  return (
    <Layout.Base title={entity.caption} structured={structured}>
      <Container>
        <h1>
          {entity.caption}
          <JSONLink href={apiUrl} />
        </h1>
        <EntityDisplay entity={entity} datasets={sources as Array<ISource>} />
      </Container>
    </Layout.Base >
  )
}


export const getStaticProps = async (context: GetStaticPropsContext) => {
  const index = await fetchIndex();
  return {
    props: {
      datasets: await getDatasets(),
      modelData: index.model
    }
  };
}
