import Link from 'next/link';
import { InferGetStaticPropsType } from 'next'
import { Model } from "@alephdata/followthemoney"
import Alert from 'react-bootstrap/Alert';

import Layout from '../components/Layout'
import Content from '../components/Content'
import { getContentBySlug } from '../lib/content'
import { Summary } from '../components/util'
import { fetchIndex } from '../lib/data'
import { SchemaReference, TypeReference } from '../components/Reference';
import { INDEX_URL } from '../lib/constants';
import { getAllParents } from '../lib/util';


export default function Reference({ content, activeModel, schemata }: InferGetStaticPropsType<typeof getStaticProps>) {
  const model = new Model(activeModel)
  const usedSchemata = schemata.map(s => model.getSchema(s)).filter(s => s !== undefined)
  const refSchemata = getAllParents(usedSchemata)

  return (
    <Layout.Content content={content}>
      <Content.Menu title={content.title} jsonLink={INDEX_URL}>
        <Summary summary={content.summary} />
        <div>
          <Content.Body content={content} />
        </div>
        <h2><a id="targets"></a>Entities and targets</h2>
        <p className="text-body">
          OpenSanctions collects data about real-world <strong><Link href="/docs/entities/">entities</Link></strong>, such as
          people, companies, sanctions and addresses, but also the relationships between
          them. In order to process that data, it is internally converted into an object
          graph that is defined below. Different exporters then might simplify the data
          for end-users.
        </p>
        <p className="text-body">
          Each data source produces a set of <strong>targets</strong>, the main entities
          described by the publisher (e.g. sanctioned companies, politicians or criminal
          actors), and any number of non-target adjacent entities. Non-target entities
          might include a polictical party that a politician is a member of, or an
          address they are linked to.
        </p>
        <p className="text-body">
          The data model used by OpenSanctions is <Link href="https://followthemoney.readthedocs.io/en/latest/index.html">FollowTheMoney</Link>,
          an ontology used in anti-corruption data analysis - in particular by the <Link href="https://docs.alephdata.org/">Aleph data platform</Link>.
          Only a subset of the entity types defined in FtM are used by OpenSanctions.
        </p>
        <Alert variant="info">
          <strong>Developer note:</strong> All of the information detailed below is also
          available in the <Link href={INDEX_URL}>JSON metadata</Link> for OpenSanctions.
          FollowTheMoney additionally provides libraries for <Link href="https://pypi.org/project/followthemoney/">Python</Link> and <Link href="https://www.npmjs.com/package/@alephdata/followthemoney">TypeScript</Link> that can be
          used to process and analyse entities more easily.
        </Alert>
        <h2><a id="schema" /> Schema types</h2>
        <p className="text-body">
          All entities in OpenSanctions must conform to a schema, a definition that states
          what properties they are allowed to have. Some properties also allow entities to
          reference other entities, turning the entities into a graph.{' '}
          <Link href="/docs/entities/">Read more about the entity graph...</Link>
        </p>
        <p className="text-body">
          The following schema types are currently referenced in OpenSanctions:
        </p>
        <ul>
          {usedSchemata.map(schema => (
            <li key={schema.name}>
              <code><Link href={`#schema.${schema.name}`}>{schema.name}</Link></code>
            </li>
          ))}
        </ul>
        <h3>Schema definitions in detail</h3>
        {refSchemata.map(schema => (
          <SchemaReference schema={schema} schemata={refSchemata} key={schema.name} />
        ))}

        <h2>Type definitions</h2>
        <p className="text-body">
          Schema properties have specific types which define the range of valid
          values they can hold. Below are the enumerated values for some of the
          types. Other types perform algorithmic validation, e.g. for phone
          numbers, URLs or email addresses.
        </p>
        <TypeReference type={model.getType('topic')}>
          Topics are used to tag other entities - mainly organizations and people - with
          semantic roles, e.g. to designate an individual as a politician, terrorist or
          to hint that a certain company is a bank.
        </TypeReference>
        <TypeReference type={model.getType('date')}>
          Dates are given in a basic ISO 8601 date or date-time format,
          <code>YYYY-MM-DD</code> or <code>YYYY-MM-DDTHH:MM:SS</code>. In source data,
          we find varying degrees of precision: some events may be defined as a
          full timestamp (<code>2021-08-25T09:26:11</code>), while for many we only
          know a year (<code>2021</code>) or month (<code>2021-08</code>). Such date
          prefixes are carried through and used to specify date precision as well as
          the actual value.
        </TypeReference>
        <TypeReference type={model.getType('country')}>
          We use a descriptive approach to modelling the countries in our database.
          If a list refers to a country, so do we. We use ISO 3166-1 alpha-2 as a
          starting point, but have also included countries that have ceased to exist (e.g.
          Soviet Union, Yugoslavia) and territories whose status
          as a country is controversial (e.g. Kosovo, Artsakh). If the presence of a
          country in this list is offensive to you, we invite you to reflect on the
          mental health impact of being angry at tables on the internet.
        </TypeReference>
      </Content.Menu>
    </Layout.Content >
  )
}

export async function getStaticProps() {
  const index = await fetchIndex()
  return {
    props: {
      content: await getContentBySlug('reference'),
      schemata: index.schemata,
      activeModel: index.model,
    }
  }
}