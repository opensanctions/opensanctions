import Link from 'next/link'
import Card from 'react-bootstrap/Card';
import { CollectionFill, MapFill } from 'react-bootstrap-icons';

import { IDataset, isCollection, isSource } from '../lib/types'
import { Markdown, Numeric } from './util';
import styles from '../styles/Dataset.module.scss'
import { SPACER } from '../lib/constants';


type DatasetProps = {
  dataset: IDataset
}

function DatasetDescription({ dataset }: DatasetProps) {
  return <Markdown markdown={dataset.description} />;
}

type DatasetIconProps = {
  dataset: IDataset
  color?: string
  size?: string
}

function DatasetIcon({ dataset, ...props }: DatasetIconProps) {
  if (isCollection(dataset)) {
    return <CollectionFill className="bsIcon" {...props} />
  }
  return <MapFill className="bsIcon" {...props} />
}


function DatasetCard({ dataset }: DatasetProps) {
  return (
    <Card key={dataset.name} className={styles.card}>
      {isCollection(dataset) && (
        <Card.Header>Collection</Card.Header>
      )}
      <Card.Body>
        <Card.Title className={styles.cardTitle}>
          <Link href={dataset.link}>
            {dataset.title}
          </Link>
        </Card.Title>
        <Card.Subtitle className="mb-2 text-muted">
          {isCollection(dataset) && (
            <><Numeric value={dataset.sources.length} /> data sources</>
          )}
          {isSource(dataset) && (
            <>{dataset.publisher.country_label}</>
          )}
          {SPACER}
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

export default class Dataset {
  static Card = DatasetCard
  static Icon = DatasetIcon
  static Description = DatasetDescription
}