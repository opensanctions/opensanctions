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
    <span className={styles.noWrap}>
      <Plural value={ts.count} one={ts.label} many={ts.plural} />
      <HelpLink href={`/reference/#schema.${ts.name}`} />
    </span>
  ), SPACER);
  return (
    <Table responsive="md">
      <tbody>
        <tr>
          <th className={styles.tableHeader}>
            Targets<HelpLink href="/reference/#targets" />:
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
            <th className={styles.tableHeader}>Publisher:</th>
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
            <th className={styles.tableHeader}>Source data:</th>
            <td>
              <URLLink url={dataset.data.url} />
              <> ({dataset.data.format})</>
            </td>
          </tr>
        )}
        {isSource(dataset) && !!collections?.length && (
          <tr>
            <th className={styles.tableHeader}>
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
          <th className={styles.tableHeader}>Last changed<HelpLink href="/docs/faq/#updates" />:</th>
          <td><FormattedDate date={dataset.last_change} /></td>
        </tr>
      </tbody>
    </Table >

  )
}
