import { useRouter } from 'next/router';
import { castArray } from 'lodash';
import { Model } from '@alephdata/followthemoney';
import Pagination from 'react-bootstrap/Pagination';
import ListGroup from 'react-bootstrap/ListGroup';
import Card from 'react-bootstrap/Card';
import Badge from "react-bootstrap/Badge";

import { IOpenSanctionsEntity, ISearchAPIResponse, ISearchFacet, OpenSanctionsEntity, Values } from "../lib/types";
import { NumericBadge } from "./util";
import { MouseEvent } from "react";
import { SPACER } from "../lib/constants";
import { EntityLink } from './Entity';
import { TypeValues } from './Property';

import styles from '../styles/Search.module.scss'


type SearchFacetProps = {
  field: string
  facet: ISearchFacet
}

export function SearchFacet({ field, facet }: SearchFacetProps) {
  const router = useRouter();
  const filters = castArray(router.query[field] || []);
  if (!facet.values.length) {
    return null;
  }

  const toggleFiltered = (value: string) => {
    const idx = filters.indexOf(value);
    const newFilters = idx === -1 ? [...filters, value] : filters.filter((e) => e !== value);
    const param = newFilters.length ? newFilters : undefined;
    router.push({
      'query': { ...router.query, [field]: param }
    })
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

  const handlePrev = (e: MouseEvent<HTMLElement>) => {
    e.preventDefault()
    router.push({ query: { ...router.query, offset: Math.max(0, response.offset - response.limit) } });
  }

  const handleNext = (e: MouseEvent<HTMLElement>) => {
    e.preventDefault()
    router.push({
      query: {
        ...router.query, offset: response.offset + response.limit
      }
    });
  }

  return (
    <Pagination>
      <Pagination.Prev disabled={!hasPrev} onClick={handlePrev} />
      <Pagination.Item disabled>
        {response.offset + 1} - {upper} of {response.total}
      </Pagination.Item>
      <Pagination.Next disabled={!hasNext} onClick={handleNext} />
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
            {SPACER}
            <Badge bg="warning"><TypeValues type={topicType} values={topics} /></Badge>
          </>
        )}
        {countries.length > 0 && (
          <>
            {SPACER}
            <TypeValues type={countryType} values={countries} />
          </>
        )}
      </p>
    </li>
  );
}
