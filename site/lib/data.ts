import { join } from 'path'
import { createInterface } from 'readline';
import { promises, createReadStream } from 'fs';
import { IModelDatum } from "@alephdata/followthemoney"
import { IDataset, ICollection, ISource, IIssueIndex, IIndex, IIssue, IOpenSanctionsEntity, IDatasetDetails } from "./types";
import { API_URL, BASE_URL, INDEX_URL, ISSUES_URL } from "./constants";
import { markdownToHtml } from './util';

export type DataCache = {
  index: IIndex | null,
  entities: Map<string, IOpenSanctionsEntity> | null
}

const dataDirectory = join(process.cwd(), '_data')
const CACHE: DataCache = { index: null, entities: null };

async function parseJsonFile(name: string): Promise<any> {
  const data = await promises.readFile(join(dataDirectory, name), 'utf8')
  return JSON.parse(data)
}

async function fetchJsonUrl(url: string): Promise<any> {
  const data = await fetch(url)
  return await data.json()
}

export async function fetchIndex(): Promise<IIndex> {
  if (CACHE.index === null) {
    // const index = await parseJsonFile('index.json');
    const index = await fetchJsonUrl(INDEX_URL);
    index.details = {};
    index.datasets = index.datasets.map((raw: any) => {
      const { description, targets, resources, ...ds } = raw;
      index.details[ds.name] = { description, targets, resources } as IDatasetDetails
      ds.link = `/datasets/${ds.name}/`
      ds.opensanctions_url = BASE_URL + ds.link
      ds.description = markdownToHtml(ds.description)
      return ds.type === 'collection' ? ds as ICollection : ds as ISource
    })
    index.model = index.model as IModelDatum
    CACHE.index = index as IIndex
  }
  return CACHE.index;
}

export async function getDatasets(): Promise<Array<IDataset>> {
  const index = await fetchIndex()
  return index.datasets
}

export async function getDatasetByName(name: string): Promise<IDataset | undefined> {
  const datasets = await getDatasets()
  return datasets.find((dataset) => dataset.name === name)
}

export async function getDatasetDetails(name: string): Promise<IDatasetDetails | undefined> {
  const index = await fetchIndex()
  return index.details[name];
}

export async function getIssues(): Promise<Array<IIssue>> {
  // const index = await parseJsonFile('issues.json') as IIssueIndex;
  const index = await fetchJsonUrl(ISSUES_URL) as IIssueIndex;
  return index.issues
}

export async function getDatasetIssues(dataset?: IDataset): Promise<Array<IIssue>> {
  const issues = await getIssues()
  return issues.filter(issue => issue.dataset === dataset?.name);
}

export async function getEntityById(id: string): Promise<IOpenSanctionsEntity | null> {
  const url = `${API_URL}/entities/${id}`
  const data = await fetch(url)
  if (!data.ok) {
    // console.log('ERROR', data);
    return null;
  }
  return await data.json()
}
