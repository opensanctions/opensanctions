import { useState, useEffect } from 'react';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'
import Container from 'react-bootstrap/Container';

import Layout from '../../components/Layout'
import { getDatasets, getDatasetByName, getDatasetIssues, getEntityById, getEntityIds } from '../../lib/data'
import { IDataset, IIssue, ICollection, ISource, isCollection, isSource, LEVEL_ERROR, LEVEL_WARNING } from '../../lib/types'
import { Summary, FileSize, NumericBadge, JSONLink, HelpLink } from '../../components/util'
import DatasetMetadataTable from '../../components/DatasetMetadataTable'
import { getSchemaDataset } from '../../lib/schema';
import { IssuesList } from '../../components/Issue';
import { SPACER } from '../../lib/constants';

import styles from '../../styles/Dataset.module.scss'
import { Entity } from '@alephdata/followthemoney';


export default function DatasetScreen({ entity }: InferGetStaticPropsType<typeof getStaticProps>) {
  if (entity === undefined) {
    return null;
  }
  return (
    <Layout.Base title={entity.caption}>
      <Container>
        <h1>{entity.caption}</h1>
        <pre>{JSON.stringify(entity, null, 2)}</pre>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const entity = await getEntityById(params.id as string);
  // if (entity !== undefined && entity.id !== params.id) {
  //   console.log("Redirect", params.id, " -> ", entity.id);
  //   return {
  //     redirect: {
  //       destination: `/entities/${entity.id}/`,
  //       permanent: false
  //     }
  //   }
  // }
  return { props: { entity: entity } };
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
