import React from 'react';
import { Model } from '@alephdata/followthemoney';
import queryString from 'query-string';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Alert from 'react-bootstrap/Alert';
import Container from 'react-bootstrap/Container';

import Layout from '../components/Layout'
import { ISearchAPIResponse } from '../lib/types';
import { fetchIndex, getDatasets } from '../lib/data';
import { GetServerSidePropsContext, InferGetServerSidePropsType } from 'next';
import { SearchFacet, SearchFilterTags, SearchPagination, SearchResultEntity } from '../components/Search';
import styles from '../styles/Search.module.scss'
import { API_URL, SEARCH_DATASET, SEARCH_SCHEMA } from '../lib/constants';
import { FormattedDate } from '../components/util';

const SUMMARY = "Provide a search term to search across sanctions lists and other persons of interest.";

export default function Search({ modelData, query, datasets, scopeName, error, response }: InferGetServerSidePropsType<typeof getServerSideProps>) {
  const model = new Model(modelData);
  const scope = datasets.find((d) => d.name === scopeName);

  if (error || scope === undefined) {
    return (
      <Layout.Base title="Failed to load">
        <Container>
          <h2>Could not load search function.</h2>
        </Container>
      </Layout.Base >
    );
  }
  const hasScope = scopeName !== SEARCH_DATASET;
  const title = hasScope ? `Search: ${scope.title}` : 'Search entities of interest';

  return (
    <Layout.Base title={title} description={SUMMARY} navSearch={false}>
      <Container>
        <Row>
          <Col md={8}>
            {!hasScope && (
              <h1>Search the OpenSanctions database</h1>
            )}
            {hasScope && (
              <h1>Search: {scope.title}</h1>
            )}
            <Form>
              <Form.Control
                name="q"
                size="lg"
                type="text"
                value={query}
                autoFocus
                className={styles.searchBox}
                placeholder="Search people, companies and other entities of interest..."
              />
            </Form>
            <SearchFilterTags scope={scope} model={model} datasets={datasets} />
            <p className={styles.searchNotice}>
              Data current as of <FormattedDate date={scope.last_change} />
            </p>
          </Col>
        </Row>
        <Row>
          <Col md={8}>
            {response.total === 0 && (
              <Alert variant="warning">
                <Alert.Heading> No matching entities were found.</Alert.Heading>
                <p>
                  Try searching a partial name, or use a different spelling.
                </p>
              </Alert>
            )}
            <ul className={styles.resultList}>
              {response.results.map((r) => (
                <SearchResultEntity key={r.id} data={r} model={model} />
              ))}
            </ul>
            <SearchPagination response={response} />
          </Col>
          <Col md={4}>
            {response.facets && response.total > 0 && (
              <>
                <SearchFacet field="topics" facet={response.facets.topics} />
                <SearchFacet field="datasets" facet={response.facets.datasets} />
                <SearchFacet field="countries" facet={response.facets.countries} />
              </>
            )}
          </Col>
        </Row>
      </Container>
    </Layout.Base >
  )
}


export const getServerSideProps = async (context: GetServerSidePropsContext) => {
  const index = await fetchIndex();
  const datasets = await getDatasets();
  const query = context.query.q || '';
  const scopeName = context.query.scope || SEARCH_DATASET;
  const schemaName = context.query.schema || SEARCH_SCHEMA;
  const apiUrl = queryString.stringifyUrl({
    'url': `${API_URL}/search/${scopeName}`,
    'query': {
      ...context.query,
      'limit': 25,
      'schema': schemaName
    }
  })

  const ret = await fetch(apiUrl)
  const response = await ret.json() as ISearchAPIResponse;

  return {
    props: {
      query,
      response,
      error: !ret.ok,
      scopeName,
      datasets: datasets,
      modelData: index.model
    }
  };
}
