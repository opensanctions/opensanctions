import Link from 'next/link'
import Card from 'react-bootstrap/Card';
import Badge from 'react-bootstrap/Badge';
import { FileEarmarkSpreadsheetFill, FolderFill } from 'react-bootstrap-icons';

import { IDataset, isCollection, isSource } from '../lib/types'
import { Numeric, NumericBadge, Spacer } from './util';
import styles from '../styles/Dataset.module.scss'


type DatasetProps = {
  dataset: IDataset
}

type DatasetIconProps = {
  dataset: IDataset
  color?: string
  size?: string
}

function DatasetIcon({ dataset, ...props }: DatasetIconProps) {
  if (isCollection(dataset)) {
    return <FolderFill className="bsIcon" {...props} />
  }
  return <FileEarmarkSpreadsheetFill className="bsIcon" {...props} />
}


function DatasetCard({ dataset }: DatasetProps) {
  return (
    <Card key={dataset.name} className={styles.card}>
      <Card.Body>
        <Card.Title className={styles.cardTitle}>
          <a href={dataset.link}>
            <span><DatasetIcon dataset={dataset} /> {dataset.title}</span>
          </a>
        </Card.Title>
        <Card.Subtitle className="mb-2 text-muted">
          {isCollection(dataset) && (
            <><Numeric value={dataset.sources.length} /> data sources</>
          )}
          {isSource(dataset) && (
            <>{dataset.publisher.country_label}</>
          )}
          <Spacer />
          <Numeric value={dataset.target_count} /> targets
        </Card.Subtitle>
        <Card.Text>
          {dataset.summary}
        </Card.Text>
        <Card.Link href={dataset.link}>Details</Card.Link>
      </Card.Body>
    </Card>
  )
}


function DatasetItem({ dataset }: DatasetProps) {
  return (
    <Card key={dataset.name} className={styles.item}>
      <Card.Body>
        <a href={dataset.link} className={styles.itemHeader}>
          <DatasetIcon dataset={dataset} /> {dataset.title}
          <NumericBadge value={dataset.target_count} className={styles.itemTargets} />
        </a>
        <p className={styles.itemDetails}>
          {isCollection(dataset) && (
            <>
              <Badge bg="light">Collection</Badge>
              <Spacer />
              <Numeric value={dataset.sources.length} /> data sources
            </>
          )}
          {isSource(dataset) && (
            <>
              {dataset.publisher.country_label}
              <Spacer />
              {dataset.publisher.name}
            </>
          )}
          {/*
          <Spacer />
          <Numeric value={dataset.target_count} /> targets
          */}
        </p>
      </Card.Body>
    </Card>
  )
}

export default class Dataset {
  static Card = DatasetCard
  static Item = DatasetItem
  static Icon = DatasetIcon
}