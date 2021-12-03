import { ISearchAPIResponse, ISearchFacet } from "../lib/types";
import Pagination from 'react-bootstrap/Pagination';
import ListGroup from 'react-bootstrap/ListGroup';
import Card from 'react-bootstrap/Card';
import { NumericBadge } from "./util";
import { useRouter } from 'next/router';

import styles from '../styles/Search.module.scss'
import { MouseEvent } from "react";

type SearchFacetProps = {
  facet: ISearchFacet
}

export function SearchFacet({ facet }: SearchFacetProps) {
  return (
    <Card className={styles.facet}>
      <Card.Header className={styles.facetHeader}>{facet.label}</Card.Header>
      <ListGroup variant="flush">
        {facet.values.map((value) => (
          <ListGroup.Item key={value.name}>
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