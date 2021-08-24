import { IModelDatum } from "@alephdata/followthemoney"
import { IDataset, IDatasetBase, ICollection, ISource, IIssueIndex } from "./dataset";
import { INDEX_URL, BASE_URL } from "./constants";

export interface IIndex {
  app: string
  version: string
  model: IModelDatum
  datasets: Array<IDataset>
}

export type IndexCache = {
  index: IIndex | null
}

const CACHE: IndexCache = { index: null };

export async function fetchIndex(): Promise<IIndex> {
  if (CACHE.index === null) {
    console.log("Fetching Index", INDEX_URL)
    const response = await fetch(INDEX_URL)
    const index = await response.json()
    index.datasets = index.datasets.map((ds: IDatasetBase) => {
      ds.link = `/datasets/${ds.name}/`
      ds.url = BASE_URL + ds.link
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

export async function getDatasetIssues(dataset?: IDataset): Promise<IIssueIndex> {
  if (dataset === undefined) {
    return { issues: [] } as IIssueIndex
  }
  const response = await fetch(dataset.issues_url)
  const data = await response.json()
  return data as IIssueIndex
}