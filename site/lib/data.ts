import { promises as fs } from 'fs';
import { join } from 'path'
import { IModelDatum } from "@alephdata/followthemoney"
import { IDataset, IDatasetBase, ICollection, ISource, IIssueIndex, IIndex, IIssue } from "./types";
import { BASE_URL } from "./constants";

export type IndexCache = {
  index: IIndex | null
}

const dataDirectory = join(process.cwd(), '_data')
const CACHE: IndexCache = { index: null };

export async function fetchIndex(): Promise<IIndex> {
  if (CACHE.index === null) {
    const data = await fs.readFile(join(dataDirectory, 'index.json'), 'utf8')
    const index = JSON.parse(data)
    index.datasets = index.datasets.map((ds: IDatasetBase) => {
      ds.link = `/datasets/${ds.name}/`
      ds.opensanctions_url = BASE_URL + ds.link
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

export async function getIssues(): Promise<Array<IIssue>> {
  const data = await fs.readFile(join(dataDirectory, 'issues.json'), 'utf8')
  const index = JSON.parse(data) as IIssueIndex
  return index.issues
}

export async function getDatasetIssues(dataset?: IDataset): Promise<Array<IIssue>> {
  const issues = await getIssues()
  return issues.filter(issue => issue.dataset === dataset?.name);
}