import Link from 'next/link'
import Card from 'react-bootstrap/Card';
import { CollectionFill, MapFill } from 'react-bootstrap-icons';

import { IDataset, ICollection, ISource, isCollection, isSource } from '../lib/dataset'
import { Markdown } from './util';
import styles from '../styles/Dataset.module.scss'


function getHref(dataset: IDataset) {
  return `/datasets/${dataset.name}/`;
}

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
          <Link href={getHref(dataset)}>
            {dataset.title}
          </Link>
        </Card.Title>
        {isCollection(dataset) && (
          <Card.Subtitle className="mb-2 text-muted">
            {dataset.sources.length} data sources
          </Card.Subtitle>
        )}
        {isSource(dataset) && (
          <Card.Subtitle className="mb-2 text-muted">
            {dataset.publisher.country_label}
          </Card.Subtitle>
        )}
        <Card.Text>
          {dataset.summary}
        </Card.Text>
        <Card.Link href={getHref(dataset)}>Details</Card.Link>
      </Card.Body>
    </Card>
  )
}

export default class Dataset {
  static getHref = getHref
  static Card = DatasetCard
  static Icon = DatasetIcon
  static Description = DatasetDescription
}