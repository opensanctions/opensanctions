//
// https://developers.google.com/search/docs/advanced/structured-data/dataset
// https://schema.org/Dataset

import { BASE_URL, LICENSE_URL, CLAIM, EMAIL, SITE } from './constants';
import { IDataset, IResource, ISourcePublisher, isSource, OpenSanctionsEntity } from './types';


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
      "creator": getPublisherOrganization(dataset.publisher),
      "maintainer": getSchemaOpenSanctionsOrganization(),
    }
    if (dataset.url) {
      schema = {
        ...schema,
        "isBasedOn": dataset.url,
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


export function getSchemaDataCatalog(datasets: Array<IDataset>) {
  return {
    ...getDataCatalog(),
    license: LICENSE_URL,
    dataset: datasets.map((d) => getSchemaDataset(d))
  }
}

function getSchemaAddress(address: OpenSanctionsEntity) {
  // https://schema.org/PostalAddress
  return {
    "@context": "https://schema.org/",
    "@type": "PostalAddress",
    "description": address.caption,
    ...applyProperty(address, 'country', 'addressCountry'),
    ...applyProperty(address, 'region', 'addressRegion'),
    ...applyProperty(address, 'city', 'addressLocality'),
    ...applyProperty(address, 'street', 'streetAddress'),
    ...applyProperty(address, 'postalCode', 'postalCode'),
    ...applyProperty(address, 'postOfficeBox', 'postOfficeBoxNumber'),
  }
}

function applyProperty(entity: OpenSanctionsEntity, prop: string, field: string) {
  if (entity.hasProperty(prop)) {
    return { [field]: entity.getProperty(prop) }
  }
  return {}
}

function getSchemaEntity(entity: OpenSanctionsEntity) {
  let schema: any = {
    "@context": "https://schema.org/",
    "@type": "Thing",
    "name": entity.caption,
    "url": `${BASE_URL}/entities/${entity.id}/`,
    ...applyProperty(entity, 'notes', 'description'),
    ...applyProperty(entity, 'alias', 'alternateName'),
    ...applyProperty(entity, 'taxNumber', 'taxID'),
    ...applyProperty(entity, 'vatCode', 'vatID'),
    ...applyProperty(entity, 'phone', 'telephone'),
    ...applyProperty(entity, 'email', 'email'),
  }
  if (entity.hasProperty('addressEntity')) {
    schema = {
      ...schema,
      'address': entity.getProperty('addressEntity').map(a => getSchemaAddress(a as OpenSanctionsEntity))
    }
  }
  const identifierType = entity.schema.model.getType('identifier');
  const identifiers = entity.getTypeValues(identifierType);
  if (identifiers.length > 0) {
    schema = {
      schema,
      'identifier': identifiers
    }
  }
  if (entity.schema.name === 'Person') {
    // https://schema.org/Person
    schema = {
      ...schema,
      "@type": "Person",
      ...applyProperty(entity, 'nationality', 'nationality'),
      ...applyProperty(entity, 'gender', 'gender'),
      ...applyProperty(entity, 'lastName', 'familyName'),
      ...applyProperty(entity, 'firstName', 'givenName'),
      ...applyProperty(entity, 'birthDate', 'birthDate'),
      ...applyProperty(entity, 'birthPlace', 'birthPlace'),
      ...applyProperty(entity, 'deathDate', 'deathDate'),
    }
  } else if (entity.schema.isA('Organization')) {
    // https://schema.org/Organization
    // TODO: jusrisdiction?
    schema = {
      ...schema,
      "@type": "Organization",
      ...applyProperty(entity, 'incorporationDate', 'foundingDate'),
      ...applyProperty(entity, 'dissolutionDate', 'dissolutionDate'),
    }
  }
  return schema;
}

export function getSchemaEntityPage(entity: OpenSanctionsEntity, datasets: Array<IDataset>) {
  const entitySchema = getSchemaEntity(entity)
  return {
    "@context": "https://schema.org/",
    "@type": "WebPage",
    "name": entity.caption,
    "maintainer": getSchemaOpenSanctionsOrganization(),
    "isPartOf": datasets.map(d => getSchemaDataset(d)),
    "license": LICENSE_URL,
    "mainEntity": entitySchema,
    "dateCreated": entity.first_seen,
    "dateModified": entity.last_seen,
  }
}