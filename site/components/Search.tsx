import { NextRouter, useRouter } from 'next/router';
import queryString from 'query-string';
import { Model } from '@alephdata/followthemoney';
import Pagination from 'react-bootstrap/Pagination';
import ListGroup from 'react-bootstrap/ListGroup';
import Card from 'react-bootstrap/Card';
import Badge from "react-bootstrap/Badge";

import { IDataset, IOpenSanctionsEntity, ISearchAPIResponse, ISearchFacet, OpenSanctionsEntity, Values } from "../lib/types";
import { NumericBadge, Spacer } from "./util";
import { SEARCH_DATASET, SEARCH_SCHEMA } from "../lib/constants";
import { EntityLink } from './Entity';
import { TypeValue, TypeValues } from './Property';

import styles from '../styles/Search.module.scss'
import { ensureArray } from '../lib/util';


type SearchFacetProps = {
  field: string
  facet: ISearchFacet
}

export function SearchFacet({ field, facet }: SearchFacetProps) {
  const router = useRouter();
  const filters = ensureArray(router.query[field]);
  if (!facet.values.length) {
    return null;
  }

  const toggleFiltered = (value: string) => {
    const idx = filters.indexOf(value);
    const newFilters = idx === -1 ? [...filters, value] : filters.filter((e) => e !== value);
    const param = newFilters.length ? newFilters : undefined;
    router.push({ 'query': { ...router.query, [field]: param } })
  }

  return (
    <Card className={styles.facet}>
      <Card.Header className={styles.facetHeader}>{facet.label}</Card.Header>
      <ListGroup variant="flush">
        {facet.values.map((value) => (
          <ListGroup.Item key={value.name}
            active={filters.indexOf(value.name) !== -1}
            onClick={(e) => toggleFiltered(value.name)}
            className={styles.facetListItem}
          >
            <NumericBadge value={value.count} bg="light" className={styles.facetCount} />
            <span className={styles.facetLabel}>{value.label}</span>
          </ListGroup.Item>
        ))}
      </ListGroup>
    </Card>
  );
}

type SearchFilterTagsProps = {
  model: Model
  scope: IDataset
  datasets: Array<IDataset>
}

export function SearchFilterTags({ scope, model, datasets }: SearchFilterTagsProps) {
  const router = useRouter();

  const unfilter = (field: string, value: string) => {
    const values = ensureArray(router.query[field]).filter((v) => v !== value);
    router.push({ 'query': { ...router.query, [field]: values } })
  }
  const filters = [];
  const schema = router.query.schema;
  if (schema !== undefined && schema !== SEARCH_SCHEMA) {
    filters.push({
      'field': 'schema',
      'value': schema as string,
      'label': model.getSchema(schema as string).plural
    })
  }
  if (scope.name !== SEARCH_DATASET) {
    filters.push({
      'field': 'scope',
      'value': scope.name,
      'label': scope.title
    })
  }
  const countries = ensureArray(router.query.countries);
  const countryType = model.getType('country');
  for (let country of countries) {
    if (country.trim().length) {
      filters.push({
        'field': 'countries',
        'value': country,
        'label': <TypeValue type={countryType} value={country} />
      })
    }
  }

  const topics = ensureArray(router.query.topics);
  const topicType = model.getType('topic');
  for (let topic of topics) {
    if (topic.trim().length) {
      filters.push({
        'field': 'topics',
        'value': topic,
        'label': <TypeValue type={topicType} value={topic} />
      })
    }
  }

  const datasetNames = ensureArray(router.query.datasets);
  for (let dataset of datasetNames) {
    const ds = datasets.find((d) => d.name == dataset)
    if (ds !== undefined) {
      filters.push({
        'field': 'datasets',
        'value': dataset,
        'label': ds.title
      })
    }
  }


  if (filters.length === 0) {
    return null;
  }

  return (
    <p className={styles.tagsSection}>
      <Badge bg="light">Filtered:</Badge>{' '}
      {filters.map((spec) =>
        <span key={`${spec.field}:${spec.value}`}>
          <Badge
            onClick={(e) => unfilter(spec.field, spec.value)}
            className={styles.tagsButton}
          >
            {spec.label}
          </Badge>
          {' '}
        </span>
      )}
    </p>
  )
}

type SearchPaginationProps = {
  response: ISearchAPIResponse
}

export function SearchPagination({ response }: SearchPaginationProps) {
  if (response.total === 0) {
    return null;
  }
  const router = useRouter();
  const nextOffset = response.offset + response.limit;
  const upper = Math.min(response.total, nextOffset);
  const hasPrev = response.offset > 0;
  const hasNext = response.total > nextOffset;

  const prevLink = queryString.stringify({
    ...router.query,
    offset: Math.max(0, response.offset - response.limit)
  })
  const nextLink = queryString.stringify({
    ...router.query,
    offset: response.offset + response.limit
  })

  return (
    <Pagination>
      <Pagination.Prev disabled={!hasPrev} href={`?${prevLink}`} />
      <Pagination.Item disabled>
        {response.offset + 1} - {upper} of {response.total}
      </Pagination.Item>
      <Pagination.Next disabled={!hasNext} href={`?${nextLink}`} />
    </Pagination>
  );
}

type SearchResultEntityProps = {
  data: IOpenSanctionsEntity
  model: Model
}

export function SearchResultEntity({ data, model }: SearchResultEntityProps) {
  const entity = OpenSanctionsEntity.fromData(model, data);
  const countryType = model.getType('country');
  const countries = entity.getTypeValues(countryType) as Values;
  const topicType = model.getType('topic');
  const topics = entity.getTypeValues(topicType) as Values;
  return (
    <li key={entity.id} className={styles.resultItem}>
      <div className={styles.resultTitle}>
        <EntityLink entity={entity} />
      </div>
      <p className={styles.resultDetails}>
        <Badge bg="light">{entity.schema.label}</Badge>
        {topics.length > 0 && (
          <>
            <Spacer />
            <Badge bg="warning"><TypeValues type={topicType} values={topics} /></Badge>
          </>
        )}
        {countries.length > 0 && (
          <>
            <Spacer />
            <TypeValues type={countryType} values={countries} />
          </>
        )}
      </p>
    </li>
  );
}
