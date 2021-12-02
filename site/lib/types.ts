import { Entity, IEntityDatum, IModelDatum, Model, Property } from "@alephdata/followthemoney"


export interface IContent {
  slug: string
  content: string
  title: string
  summary: string | null
}

export interface IArticleInfo {
  slug: string
  title: string
  date: string
  path: string
  url: string
  draft: boolean
  summary: string | null
}

export interface IArticle extends IArticleInfo {
  content: string
}

export interface IResource {
  url: string
  path: string
  sha1: string
  timestamp: string
  dataset: string
  mime_type: string
  mime_type_label: string
  size: number
  title: string
}

export interface IIssueType {
  warning: number
  error: number
}

export interface ITargetCountry {
  code: string
  count: number
  label: string
}

export interface ITargetSchema {
  name: string
  count: number
  label: string
  plural: string
}

export interface ITargetStats {
  countries: Array<ITargetCountry>
  schemata: Array<ITargetSchema>
}

export interface IDatasetDetails {
  description?: string
  targets: ITargetStats
  resources: Array<IResource>
}


export interface IDatasetBase {
  name: string
  type: string
  title: string
  link: string
  opensanctions_url: string
  summary: string
  index_url: string
  last_change: string
  last_export: string
  issue_count: number
  issue_levels: IIssueType
  issues_url: string
  target_count: number
}

export interface ISourceData {
  url?: string
  format?: string
  model?: string
}

export interface ISourcePublisher {
  url?: string
  name: string
  description: string
  country?: string
  country_label?: string
}


export interface ISource extends IDatasetBase {
  url?: string
  data: ISourceData
  publisher: ISourcePublisher
  collections: Array<string>
}

export interface ICollection extends IDatasetBase {
  sources: Array<string>
}

export type IDataset = ISource | ICollection

export function isCollection(dataset?: IDataset): dataset is ICollection {
  return dataset?.type === 'collection';
}

export function isSource(dataset?: IDataset): dataset is ISource {
  return dataset?.type === 'source';
}

export const LEVEL_ERROR = 'error'
export const LEVEL_WARNING = 'warning'

export interface IIssue {
  id: number
  level: string
  message: string
  module: string
  timestamp: string
  data: { [key: string]: string }
  dataset: string
  entity_id?: string | null
  entity_schema?: string | null
}

export interface IIssueIndex {
  issues: Array<IIssue>
}

export interface IIndex {
  app: string
  version: string
  model: IModelDatum
  issues_url: string
  schemata: Array<string>
  datasets: Array<IDataset>
  details: { [key: string]: IDatasetDetails }
}

export interface IOpenSanctionsEntity extends IEntityDatum {
  caption: string
  referents: Array<string>
  datasets: Array<string>
  last_seen: string
  first_seen: string
  target: boolean
  properties: EntityProperties
}

export type Value = string | OpenSanctionsEntity
export type Values = Array<Value>
export type EntityProperties = { [prop: string]: Array<Value> }


export class OpenSanctionsEntity extends Entity {
  public caption: string
  public first_seen: string
  public last_seen: string
  public referents: Array<string>
  public datasets: Array<string>
  public target: boolean
  // public properties: Map<Property, Values> = new Map()

  constructor(model: Model, data: IOpenSanctionsEntity) {
    super(model, data)
    this.caption = data.caption
    this.first_seen = data.first_seen
    this.last_seen = data.last_seen
    this.referents = data.referents
    this.datasets = data.datasets
    this.target = data.target
  }

  setProperty(prop: string | Property, value: Value): Values {
    const property = this.schema.getProperty(prop)
    const values = this.properties.get(property) || []
    if (value === undefined || value === null) {
      return values as Values
    }
    if (property.type.name === 'entity') {
      if (typeof (value) === 'string') {
        // don't allow setting stringy entity properties.
        // this may backfire later.
        return values as Values
      }
      const entity = value as unknown as IOpenSanctionsEntity
      value = new OpenSanctionsEntity(this.schema.model, entity)
    }
    values.push(value)
    // console.log('set', property, values);
    this.properties.set(property, values)
    return values as Values
  }

  hasProperty(prop: string | Property): boolean {
    try {
      const property = this.schema.getProperty(prop)
      return this.properties.has(property)
    } catch {
      return false
    }
  }

  getProperty(prop: string | Property): Values {
    try {
      const property = this.schema.getProperty(prop)
      if (!this.properties.has(property)) {
        return []
      }
      return this.properties.get(property) as Values
    } catch {
      return []
    }
  }

  getDisplayProperties(): Array<Property> {
    const properties = this.schema.getFeaturedProperties();
    const existingProps = this.getProperties().sort((a, b) => a.label.localeCompare(b.label))
    for (let prop of existingProps) {
      if (properties.indexOf(prop) == -1) {
        properties.push(prop)
      }
    }
    return properties.filter((p) => !p.hidden);
  }

  static fromData(model: Model, data: IOpenSanctionsEntity): OpenSanctionsEntity {
    return new OpenSanctionsEntity(model, data)
  }
}