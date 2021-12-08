import React, { FormEvent, useState } from 'react';
import { Model } from '@alephdata/followthemoney';
import { useRouter } from 'next/router'
import useSWR from 'swr';
import queryString from 'query-string';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Alert from 'react-bootstrap/Alert';
import Container from 'react-bootstrap/Container';

import Layout from '../components/Layout'
import { ISearchAPIResponse } from '../lib/types';
import { fetchIndex, getDatasets } from '../lib/data';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next';
import { SearchFacet, SearchFilterTags, SearchPagination, SearchResultEntity } from '../components/Search';
import styles from '../styles/Search.module.scss'
import { swrFetcher } from '../lib/util';
import { API_URL, SEARCH_DATASET, SEARCH_SCHEMA } from '../lib/constants';
import { FormattedDate, SectionSpinner } from '../components/util';

const SUMMARY = "Provide a search term to search across sanctions lists and other persons of interest.";

export default function Search({ modelData, datasets }: InferGetStaticPropsType<typeof getStaticProps>) {
  const model = new Model(modelData);
  const router = useRouter();
  const [query, setQuery] = useState<string | null>(null);
  const realQuery = query === null ? router.query.q || '' : query;
  const scopeName = router.query.scope || SEARCH_DATASET;
  const scope = datasets.find((d) => d.name === scopeName);
  const schemaName = router.query.schema || SEARCH_SCHEMA;
  const apiUrl = queryString.stringifyUrl({
    'url': `${API_URL}/search/${scopeName}`,
    'query': {
      ...router.query,
      'limit': 25,
      'schema': schemaName
    }
  })
  const { data, error } = useSWR(apiUrl, swrFetcher)
  const response = data ? data as ISearchAPIResponse : undefined
  const isLoading = !data && !error;

  if (scope === undefined || error) {
    return (
      <Layout.Base title="Failed to load">
        <Container>
          <h2>Could not load search function.</h2>
        </Container>
      </Layout.Base >
    );
  }
  const hasScope = scopeName !== SEARCH_DATASET;
  // const hasSchemaFilter = schema !== SEARCH_SCHEMA;
  // const hasFilter = hasScopeFilter || hasSchemaFilter;
  const title = hasScope ? `Search: ${scope.title}` : 'Search entities of interest';

  const handleSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    router.push({ query: { ...router.query, q: realQuery, offset: 0 } });
  }

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
            <Form onSubmit={handleSubmit}>
              <Form.Control
                name="q"
                value={realQuery}
                size="lg"
                type="text"
                autoFocus
                onChange={(e) => setQuery(e.target.value)}
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
            {isLoading && (
              <SectionSpinner />
            )}
            {response && response.results && (
              <>
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
              </>
            )}
          </Col>
          <Col md={4}>
            {response && response.facets && response.total > 0 && (
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


export const getStaticProps = async (context: GetStaticPropsContext) => {
  const index = await fetchIndex();
  return {
    props: {
      datasets: await getDatasets(),
      modelData: index.model
    }
  };
}
