//
// https://developers.google.com/search/docs/advanced/structured-data/dataset
// https://schema.org/Dataset

import { BASE_URL, LICENSE_URL, CLAIM, EMAIL, SITE } from './constants';
import { fetchIndex, getDatasetByName, getDatasets } from './api';
import { IResource, isCollection, ISourcePublisher, isSource } from './dataset';


export function getSchemaOpenSanctionsOrganization() {
  return {
    "@context": "https://schema.org/",
    "@type": "Organization",
    "name": SITE,
    "url": BASE_URL,
    "email": EMAIL,
    "description": CLAIM,
    "license": LICENSE_URL,
    "funder": "https://ror.org/04pz7b180"
  }
}

function getDataCatalog() {
  return {
    "@context": "https://schema.org/",
    "@type": "DataCatalog",
    "name": SITE,
    "url": `${BASE_URL}/datasets/`,
    "creator": getSchemaOpenSanctionsOrganization()
  }
}

function getPublisherOrganization(publisher: ISourcePublisher) {
  return {
    "@context": "https://schema.org/",
    "@type": "Organization",
    "name": publisher.name,
    "url": publisher.url,
    "description": publisher.description,
  }
}

function getResourceDataDownload(resource: IResource) {
  return {
    "@context": "https://schema.org/",
    "@type": "DataDownload",
    "name": resource.title,
    "contentUrl": resource.url,
    "encodingFormat": resource.mime_type,
    "uploadDate": resource.timestamp,
  }
}


async function getNestedSchemaDatasets(datasets: Array<string>, deep: boolean): Promise<any> {
  const nested = await Promise.all(datasets.map((name) => getDatasetByName(name)))
  if (deep) {
    return await Promise.all(nested.map((d) => d ? getSchemaDataset(d.name, false) : undefined))
  }
  return nested.map((d) => d?.url)
}

export async function getSchemaDataset(name: string, deep: boolean = true) {
  const dataset = await getDatasetByName(name)
  const index = await fetchIndex()
  if (dataset === undefined) {
    return undefined
  }
  let schema: any = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "name": dataset.title,
    "url": dataset.url,
    "description": dataset.summary,
    "license": LICENSE_URL,
    "version": index.version,
    "includedInDataCatalog": getDataCatalog(),
    "creator": getSchemaOpenSanctionsOrganization(),
    "isAccessibleForFree": true,
    "dateModified": dataset.last_change,
    "distribution": dataset.resources.map((r) => getResourceDataDownload(r))
  }
  if (isSource(dataset)) {
    schema = {
      ...schema,
      "isBasedOn": dataset.data.url,
      "isPartOf": await getNestedSchemaDatasets(dataset.collections, deep),
      "sameAs": dataset.url,
      "publisher": getPublisherOrganization(dataset.publisher)
    }
    if (dataset.publisher.country !== 'zz') {
      schema = {
        ...schema,
        "countryOfOrigin": dataset.publisher.country,
      }
    }
  }
  if (isCollection(dataset)) {
    schema = {
      ...schema,
      "hasPart": await getNestedSchemaDatasets(dataset.sources, deep),
    }
  }
  return schema;
}


export async function getSchemaDataCatalog() {
  const datasetObjs = await getDatasets()
  const datasets = await Promise.all(datasetObjs.map((d) => getSchemaDataset(d.name, false)))
  return {
    ...getDataCatalog(),
    license: LICENSE_URL,
    dataset: datasets
  }
}