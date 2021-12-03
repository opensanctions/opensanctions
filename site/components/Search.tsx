import { ISearchFacet } from "../lib/types";
import ListGroup from 'react-bootstrap/ListGroup';
import Card from 'react-bootstrap/Card';
import { NumericBadge } from "./util";

import styles from '../styles/Search.module.scss'

type SearchFacetProps = {
  facet: ISearchFacet
}

export function SearchFacet({ facet }: React.PropsWithChildren<SearchFacetProps>) {
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