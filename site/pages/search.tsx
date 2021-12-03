import React, { FormEvent, useState } from 'react';
import { Model } from '@alephdata/followthemoney';
import { useRouter } from 'next/router'
import Link from 'next/link';
import useSWR from 'swr'
import queryString from 'query-string'

import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Spinner from 'react-bootstrap/Spinner';
import Container from 'react-bootstrap/Container';

import Layout from '../components/Layout'

import { ISearchAPIResponse } from '../lib/types';
import { fetchIndex, getDatasets } from '../lib/data';
import { GetStaticPropsContext, InferGetStaticPropsType } from 'next';
import { SearchFacet } from '../components/Search';


const SUMMARY = "Provide a search term to search across sanctions lists and other persons of interest.";
const fetcher = (url: string) => fetch(url).then(res => res.json())

export default function Search({ modelData, datasets }: InferGetStaticPropsType<typeof getStaticProps>) {
  const model = new Model(modelData);
  const router = useRouter();
  const [query, setQuery] = useState<string | null>(null);
  const realQuery = query === null ? router.query.q || '' : query;
  const apiUrl = queryString.stringifyUrl({
    // 'url': 'https://api.opensanctions.org/search/default',
    'url': 'http://localhost:8000/search/default',
    'query': { 'q': router.query.q, 'limit': 25, 'schema': 'Thing' }
  })
  const { data, error } = useSWR(apiUrl, fetcher)
  const response = data ? data as ISearchAPIResponse : undefined
  const isLoading = !data && !error;

  const handleSubmit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    router.push({ query: { q: realQuery, offset: 0 } });
  }

  return (
    <Layout.Base title="Search" description={SUMMARY} >
      <Container>
        <Row>
          <Col md={8}>
            <h1>Search the OpenSanctions database</h1>
            <Form onSubmit={handleSubmit}>
              <Form.Control
                name="q"
                value={realQuery}
                size="lg"
                type="text"
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search people, companies and other entities of interest..."
              />
            </Form>
          </Col>
        </Row>
        <Row>
          <Col md={8}>
            {isLoading && (
              <Spinner animation="grow" variant="primary" />
            )}
            {response && response.results && (
              <ul>
                {response.results.map((r) => <li key={r.id}>{r.caption} [{r.schema}]</li>)}
              </ul>
            )}
          </Col>
          <Col md={4}>
            {response && response.facets && (
              <>
                <SearchFacet facet={response.facets.topics} />
                <SearchFacet facet={response.facets.datasets} />
                <SearchFacet facet={response.facets.countries} />
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
