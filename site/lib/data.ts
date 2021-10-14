import { join } from 'path'
import { createInterface } from 'readline';
import { promises, createReadStream } from 'fs';
import { IModelDatum } from "@alephdata/followthemoney"
import { IDataset, IDatasetBase, ICollection, ISource, IIssueIndex, IIndex, IIssue, IOpenSanctionsEntity } from "./types";
import { BASE_URL } from "./constants";

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

export async function fetchIndex(): Promise<IIndex> {
  if (CACHE.index === null) {
    const index = await parseJsonFile('index.json');
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
  const index = await parseJsonFile('issues.json') as IIssueIndex;
  return index.issues
}

export async function getDatasetIssues(dataset?: IDataset): Promise<Array<IIssue>> {
  const issues = await getIssues()
  return issues.filter(issue => issue.dataset === dataset?.name);
}

function getEntityMap(): Promise<Map<string, IOpenSanctionsEntity>> {
  const promise = new Promise<Map<string, IOpenSanctionsEntity>>((resolve) => {
    if (CACHE.entities !== null) {
      return resolve(CACHE.entities);
    }
    const entities = new Map<string, any>();
    const fileStream = createReadStream(join(dataDirectory, 'targets.ijson'));
    const lineReader = createInterface(fileStream);
    lineReader.on('line', (line) => {
      const entity = JSON.parse(line) as IOpenSanctionsEntity
      entities.set(entity.id, entity);
      for (let aliasId of entity.referents) {
        entities.set(aliasId, entity);
      }
    });
    lineReader.on('close', () => {
      CACHE.entities = entities;
      // console.log("Entities loaded!", entities.size);
      resolve(entities);
    });
  });
  return promise;
}

export async function getEntityById(id: string): Promise<IOpenSanctionsEntity | undefined> {
  const entities = await getEntityMap();
  return entities.get(id)
}

export async function getEntityIds(): Promise<Array<string>> {
  const entities = await getEntityMap();
  return Array.from(entities.keys())
}