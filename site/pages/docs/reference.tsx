import { InferGetStaticPropsType } from 'next'
import { Model } from "@alephdata/followthemoney"
import Table from 'react-bootstrap/Table';

import Layout from '../../components/Layout'
import Content from '../../components/Content'
import { getContentBySlug } from '../../lib/content'
import { Summary } from '../../components/util'
import { fetchIndex, getDatasetByName } from '../../lib/api'

export default function Reference({ content, dataset, model }: InferGetStaticPropsType<typeof getStaticProps>) {
  if (dataset === undefined) {
    return null;
  }
  const ftm = new Model(model)
  const country = ftm.getType('country')
  const countryValues = Array.from(country.values.entries())

  const schemata = dataset.targets.schemata.map(s => ftm.getSchema(s.name)).filter(s => s !== undefined)

  return (
    <Layout.Content content={content}>
      <Content.Menu title={content.title}>
        <Summary summary={content.summary} />
        <div>
          <Content.Body content={content} />
        </div>
        <h2>Schema types</h2>
        {schemata.map((schema) => (
          <>
            <h3><a id={`schema.${schema.name}`} /> {schema.plural}</h3>
            <Table>
              <thead>
                <tr>
                  <th>Property</th>
                  <th>Type</th>
                  <th>Label</th>
                  <th>Description</th>
                </tr>
              </thead>
              {/* <tbody>
                {schema.getProperties().values().map((prop) => (
                  <tr key={prop.qname}>

                  </tr>
                ))}
              </tbody> */}
            </Table>
          </>
        ))}

        <h2>Type definitions</h2>
        <h3><a id={country.name} />{country.plural}</h3>
        <p>
          Country index.
        </p>
        <Table>
          <thead>
            <tr>
              <th>Code</th>
              <th>Label</th>
            </tr>
          </thead>
          <tbody>
            {countryValues.map(([code, label]) => (
              <tr key={code}>
                <td><code>{code}</code></td>
                <td>{label}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      </Content.Menu>
    </Layout.Content >
  )
}

export async function getStaticProps() {
  const index = await fetchIndex()
  return {
    props: {
      content: await getContentBySlug('reference'),
      model: index.model,
      dataset: await getDatasetByName('all')
    }
  }
}