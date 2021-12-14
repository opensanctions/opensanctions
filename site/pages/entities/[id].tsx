import { Model } from '@alephdata/followthemoney';
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import { ISource, isSource, OpenSanctionsEntity } from '../../lib/types';
import { fetchIndex, getDatasets, getEntityById } from '../../lib/data';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next';
import { API_URL } from '../../lib/constants';
import { JSONLink } from '../../components/util';
import { getSchemaEntityPage } from '../../lib/schema';
import { EntityDisplay, EntityRedirect } from '../../components/Entity';

// import styles from '../styles/Search.module.scss'


export default function Entity({ entityId, entityData, modelData, sources }: InferGetStaticPropsType<typeof getStaticProps>) {
  const model = new Model(modelData);
  if (entityData === null || entityData.id === undefined) {
    return (
      <Layout.Base title="Failed to load">
        <Container>
          <h1 className="errorTitle">
            Could not load: {entityId}
          </h1>
        </Container>
      </Layout.Base >
    );
  }
  if (entityData.id !== entityId) {
    return <EntityRedirect entity={entityData} />
  }
  const entity = OpenSanctionsEntity.fromData(model, entityData)
  const structured = getSchemaEntityPage(entity, sources)
  const apiUrl = `${API_URL}/entities/${entityId}`

  return (
    <Layout.Base title={entity.caption} structured={structured}>
      <Container>
        <h1>
          {entity.caption}
          <JSONLink href={apiUrl} />
        </h1>
        <EntityDisplay entity={entity} datasets={sources} />
      </Container>
    </Layout.Base >
  )
}


export const getStaticProps = async (context: GetStaticPropsContext) => {
  const entityId = context.params?.id as (string | undefined);
  if (entityId === undefined) {
    return { redirect: { destination: '/search/', permanent: false } };
  }
  const index = await fetchIndex();
  const datasets = await getDatasets();
  const entity = await getEntityById(entityId);
  const sourceNames = entity !== null ? entity.datasets : [];

  const sources = sourceNames
    .map((name) => datasets.find((d) => d.name === name))
    .filter((d) => d !== undefined)
    .filter((d) => isSource(d)) as Array<ISource>

  return {
    props: {
      entityId,
      sources: sources,
      entityData: entity,
      modelData: index.model
    },
    revalidate: 360
  };
}


export async function getStaticPaths() {
  return {
    paths: [],
    fallback: 'blocking'
  }
}
