import { useState, useEffect } from 'react';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next'
import Link from 'next/link';
import { useRouter } from 'next/router';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Card from 'react-bootstrap/Card';
import Table from 'react-bootstrap/Table';
import Tabs from 'react-bootstrap/Tabs';
import Tab from 'react-bootstrap/Tab';
import Alert from 'react-bootstrap/Alert';
import Tooltip from 'react-bootstrap/Tooltip';
import ListGroup from 'react-bootstrap/ListGroup';
import Container from 'react-bootstrap/Container';
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import { FileEarmarkArrowDownFill } from 'react-bootstrap-icons';

import Layout from '../../components/Layout'
import Dataset from '../../components/Dataset'
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
    <Layout.Base title={entity.getCaption()}>
      <Container>
        <h1>
          {entity.getCaption()}
        </h1>
      </Container>
    </Layout.Base >
  )
}

export const getStaticProps = async (context: GetStaticPropsContext) => {
  const params = context.params!
  const entity = await getEntityById(params.id as string);
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
