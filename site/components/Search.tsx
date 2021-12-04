import { useRouter } from 'next/router';
import useSWR from 'swr';
import { Model } from '@alephdata/followthemoney';
import Pagination from 'react-bootstrap/Pagination';
import ListGroup from 'react-bootstrap/ListGroup';
import Container from 'react-bootstrap/Container';
import Card from 'react-bootstrap/Card';
import Modal from "react-bootstrap/Modal";
import Badge from "react-bootstrap/Badge";

import { IDataset, IOpenSanctionsEntity, ISearchAPIResponse, ISearchFacet, OpenSanctionsEntity, Values } from "../lib/types";
import { NumericBadge, SectionSpinner } from "./util";
import { MouseEvent } from "react";
import { API_URL, SPACER } from "../lib/constants";
import { swrFetcher } from "../lib/util";
import { EntityDisplay } from './Entity';

import styles from '../styles/Search.module.scss'
import { TypeValues } from './Property';


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
  const router = useRouter();
  const entity = OpenSanctionsEntity.fromData(model, data);
  const countryType = model.getType('country');
  const countries = entity.getTypeValues(countryType) as Values;
  const topicType = model.getType('topic');
  const topics = entity.getTypeValues(topicType) as Values;

  const handleClickEntity = (e: MouseEvent<HTMLElement>) => {
    e.preventDefault()
    router.push({
      query: { ...router.query, entity: entity.id }
    });
  }

  return (
    <li key={entity.id} className={styles.resultItem}>
      <div className={styles.resultTitle}>
        <a onClick={(e) => handleClickEntity(e)} href={`/entities/${entity.id}/`}>
          {entity.caption}
        </a>
      </div>
      <p className={styles.resultDetails}>
        <Badge bg="light">{entity.schema.label}</Badge>
        {topics.length > 0 && (
          <>
            {SPACER}
            <Badge bg="warning"><TypeValues type={topicType} values={topics} /></Badge>
          </>
        )}
        {SPACER}
        <TypeValues type={countryType} values={countries} />
      </p>
    </li>
  );
}




type SearchEntityModalProps = {
  entityId: string
  datasets: Array<IDataset>
  model: Model
}

export function SearchEntityModal({ entityId, datasets, model }: SearchEntityModalProps) {
  const router = useRouter();
  const { data, error } = useSWR(`${API_URL}/entities/${entityId}`, swrFetcher)

  const handleClose = () => {
    router.push({ query: { ...router.query, entity: undefined } });
  }

  if (!data) {
    return (
      <Modal show dialogClassName="modal-wide" onHide={handleClose}>
        <Modal.Body>
          <SectionSpinner />
        </Modal.Body>
      </Modal>
    );
  }

  const entity = OpenSanctionsEntity.fromData(model, data)
  const sources = entity.datasets
    .map((name) => datasets.find((d) => d.name === name))
    .filter((d) => d !== undefined)

  return (
    <Modal show dialogClassName="modal-wide" onHide={handleClose}>
      <Modal.Header closeButton>{entity.caption}</Modal.Header>
      <Modal.Body>
        <Container>
          <EntityDisplay entity={entity} datasets={sources as Array<IDataset>} />
        </Container>
      </Modal.Body>
    </Modal>
  );
}
