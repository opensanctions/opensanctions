//
// https://developers.google.com/search/docs/advanced/structured-data/dataset
// https://schema.org/Dataset

import { BASE_URL, LICENSE_URL, CLAIM, EMAIL, SITE } from './constants';
import { IDataset, IResource, ISourcePublisher, isSource } from './types';


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

export function getSchemaDataset(dataset: IDataset) {
  let schema: any = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "name": dataset.title,
    "url": dataset.opensanctions_url,
    "description": dataset.summary,
    "license": LICENSE_URL,
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
    if (dataset.publisher.country !== 'zz') {
      schema = {
        ...schema,
        "countryOfOrigin": dataset.publisher.country,
      }
    }
  }
  return schema;
}


export async function getSchemaDataCatalog(datasets: Array<IDataset>) {
  return {
    ...getDataCatalog(),
    license: LICENSE_URL,
    dataset: datasets.map((d) => getSchemaDataset(d))
  }
}

// function applyProperty(entity, prop, field) {

// }

// export async function getSchemaEntity(entity) {
//   // https://schema.org/Person
//   // https://schema.org/Organization
//   // https://schema.org/PostalAddress
// }