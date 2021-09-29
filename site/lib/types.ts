import { IModelDatum } from "@alephdata/followthemoney"

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

export interface IDatasetBase {
  name: string
  type: string
  title: string
  link: string
  opensanctions_url: string
  summary: string
  description?: string
  index_url: string
  last_change: string
  last_export: string
  issue_count: number
  issue_levels: IIssueType
  issues_url: string
  target_count: number
  targets: ITargetStats
  resources: Array<IResource>
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
}

