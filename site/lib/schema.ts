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


async function getNestedSchemaDatasets(datasets: Array<string>): Promise<any> {
  return await Promise.all(datasets.map((name) => getSchemaDataset(name, false)))
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
    "url": dataset.opensanctions_url,
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
      "publisher": getPublisherOrganization(dataset.publisher)
    }
    if (dataset.url) {
      schema = {
        ...schema,
        "sameAs": dataset.url,
      }
    }
    if (deep) {
      schema = {
        ...schema,
        "isPartOf": await getNestedSchemaDatasets(dataset.collections),
      }
    }
    if (dataset.publisher.country !== 'zz') {
      schema = {
        ...schema,
        "countryOfOrigin": dataset.publisher.country,
      }
    }
  }
  if (isCollection(dataset) && deep) {
    schema = {
      ...schema,
      "hasPart": await getNestedSchemaDatasets(dataset.sources),
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