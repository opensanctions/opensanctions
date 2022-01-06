import React from 'react';
import Link from 'next/link';
import queryString from 'query-string';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Table from 'react-bootstrap/Table';
import Container from 'react-bootstrap/Container';

import Layout from '../components/Layout'
import { IStatementAPIResponse } from '../lib/types';
import { GetServerSidePropsContext, InferGetServerSidePropsType } from 'next';
import { ResponsePagination } from '../components/util';
import styles from '../styles/Statement.module.scss'
import { API_URL } from '../lib/constants';
import { FormattedDate, JSONLink } from '../components/util';
import { AspectRatioFill, Link45deg } from 'react-bootstrap-icons';

type ExpandProps = {
  href: string
}

function Expand({ href }: ExpandProps) {
  return (
    <td className={styles.colExpand}>
      <a href={href}><AspectRatioFill size={14} /></a>
    </td>
  )

}

type StatementValueProps = {
  value: string
  prop: string
  propType: string
}

function StatementValue({ value, prop, propType }: StatementValueProps) {
  const filterQuery = (args: any) => {
    const newQuery = queryString.stringify({
      ...args
    });
    return `?${newQuery}`;
  }

  if (propType === 'url') {
    return (
      <a href={value}>
        <Link45deg /> {value}
      </a>
    );
  }

  if (propType === 'entity' || prop === 'id') {
    return (
      <Link href={filterQuery({ entity_id: value })}>
        {value}
      </Link>
    );
  }
  return <>{value}</>;
}


export default function Statements({ apiUrl, error, response }: InferGetServerSidePropsType<typeof getServerSideProps>) {
  if (error) {
    return (
      <Layout.Base title="Failed to load">
        <Container>
          <h2>Could not load raw data viewer</h2>
        </Container>
      </Layout.Base >
    );
  }
  const title = 'Raw data explorer';

  const filterQuery = (args: any) => {
    const newQuery = queryString.stringify({
      ...args
    });
    return `?${newQuery}`;
  }

  return (
    <Layout.Base title={title} navSearch={true}>
      <Container>
        <Row>
          <Col md={12}>
            <h1>
              {title}
              <JSONLink href={apiUrl} />
            </h1>
          </Col>
        </Row>
        <Row>
          <Col md={12}>
            <Table bordered size="sm">
              <thead>
                <tr>
                  <th className={styles.colCanonical} colSpan={2}>ID</th>
                  <th className={styles.colProp} colSpan={2}>Property</th>
                  <th className={styles.colValue}>Value</th>
                  <th className={styles.colDataset} colSpan={2}>Source dataset</th>
                  <th className={styles.colEntity}>Source ID</th>
                  <th className={styles.colDate}>First seen</th>
                  <th className={styles.colDate}>Last seen</th>
                </tr>
              </thead>
              <tbody>
                {response.results.map((stmt, idx) => (
                  <tr key={`stmt-${idx}`}>
                    <td className={styles.colCanonical}>
                      <Link href={filterQuery({ canonical_id: stmt.canonical_id })}>
                        {stmt.canonical_id}
                      </Link>
                    </td>
                    <Expand href={`/entities/${stmt.canonical_id}`} />
                    <td className={styles.colProp}>
                      <code><span className="text-muted">{stmt.schema}:</span>{stmt.prop}</code>
                    </td>
                    <Expand href={`/reference/#schema.${stmt.schema}`} />
                    <td className={styles.colValue}>
                      <StatementValue value={stmt.value} prop={stmt.prop} propType={stmt.prop_type} />
                    </td>
                    <td className={styles.colDataset}>
                      <Link href={filterQuery({ dataset: stmt.dataset })}>
                        {stmt.dataset}
                      </Link>
                    </td>
                    <Expand href={`/datasets/${stmt.dataset}/`} />
                    <td className={styles.colEntity}>
                      <Link href={filterQuery({ entity_id: stmt.entity_id })}>
                        {stmt.entity_id}
                      </Link>
                    </td>
                    <td className={styles.colDate}>
                      <FormattedDate date={stmt.first_seen} />
                    </td>
                    <td className={styles.colDate}>
                      <FormattedDate date={stmt.last_seen} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </Table>
            <ResponsePagination response={response} />
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}


export const getServerSideProps = async (context: GetServerSidePropsContext) => {
  // const index = await fetchIndex();
  // const datasets = await getDatasets();
  const apiUrl = queryString.stringifyUrl({
    'url': `${API_URL}/statements`,
    'query': {
      ...context.query,
      'limit': 100,
    }
  })

  const ret = await fetch(apiUrl)
  const response = await ret.json() as IStatementAPIResponse;
  return {
    props: {
      response,
      // query: context.query,
      apiUrl,
      error: !ret.ok,
      // datasets: datasets,
      // modelData: index.model
    }
  };
}
