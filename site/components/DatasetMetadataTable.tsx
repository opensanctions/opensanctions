import Link from 'next/link'
import Table from 'react-bootstrap/Table'

import { IDataset, ICollection, isSource } from '../lib/dataset'
import { FormattedDate, HelpLink, Numeric, Plural, URLLink } from './util'
import { wordList } from '../lib/util'
import { SPACER } from '../lib/constants'

import styles from '../styles/Dataset.module.scss'


type DatasetScreenProps = {
  dataset: IDataset
  collections?: Array<ICollection>
}

export default function DatasetMetadataTable({ dataset, collections }: DatasetScreenProps) {
  const schemaList = wordList(dataset.targets.schemata.map((ts) =>
    <span>
      <Plural value={ts.count} one={ts.label} many={ts.plural} />
      <HelpLink href={`/docs/reference/#schema.${ts.name}`} />
    </span >
  ), SPACER);
  return (
    <Table>
      <tbody>
        <tr>
          <th style={{ width: "15%" }}>
            Targets<HelpLink href="/docs/reference/#targets" />:
          </th>
          <td>
            {dataset.targets.schemata.length == 1 && schemaList}
            {dataset.targets.schemata.length > 1 && (
              <>
                {schemaList}
                <> (<Numeric value={dataset.target_count} /> total)</>
              </>
            )}
          </td>
        </tr>
        {isSource(dataset) && (
          <tr>
            <th>Publisher:</th>
            <td>
              <URLLink url={dataset.publisher.url} label={dataset.publisher.name} icon={false} />
              {dataset.publisher.country !== 'zz' && (
                <> ({dataset.publisher.country_label})</>
              )}
              <p className={styles.publisherDescription}>{dataset.publisher.description}</p>
            </td>
          </tr>
        )}
        {isSource(dataset) && dataset.data.url && (
          <tr>
            <th>Source data:</th>
            <td>
              <URLLink url={dataset.data.url} />
              <> ({dataset.data.format})</>
            </td>
          </tr>
        )}
        {isSource(dataset) && !!collections?.length && (
          <tr>
            <th>
              Collections<HelpLink href="/docs/faq/#collections" />:
            </th>
            <td>
              <>included in </>
              {wordList(collections.map((collection) =>
                <Link href={collection.link}>
                  {collection.title}
                </Link>
              ), SPACER)}
            </td>
          </tr>
        )}
        <tr>
          <th>Last changed<HelpLink href="/docs/faq/#updates" />:</th>
          <td><FormattedDate date={dataset.last_change} /></td>
        </tr>
      </tbody>
    </Table >

  )
}
