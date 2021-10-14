import { promises, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { join } from 'path'
import { Entity, IEntityDatum, IModelDatum, Model } from "@alephdata/followthemoney"
import { IDataset, IDatasetBase, ICollection, ISource, IIssueIndex, IIndex, IIssue } from "./types";
import { BASE_URL } from "./constants";

export type DataCache = {
  index: IIndex | null,
  entities: Map<string, Entity> | null
}

const dataDirectory = join(process.cwd(), '_data')
const CACHE: DataCache = { index: null, entities: null };

export async function fetchIndex(): Promise<IIndex> {
  if (CACHE.index === null) {
    const data = await promises.readFile(join(dataDirectory, 'index.json'), 'utf8')
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

export async function getModel(): Promise<Model> {
  const index = await fetchIndex()
  return new Model(index.model);
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
  const data = await promises.readFile(join(dataDirectory, 'issues.json'), 'utf8')
  const index = JSON.parse(data) as IIssueIndex
  return index.issues
}

export async function getDatasetIssues(dataset?: IDataset): Promise<Array<IIssue>> {
  const issues = await getIssues()
  return issues.filter(issue => issue.dataset === dataset?.name);
}

function getEntityMap(): Promise<Map<string, Entity>> {
  const fileStream = createReadStream(join(dataDirectory, 'targets.ijson'));
  const lineReader = createInterface(fileStream);
  const promise = new Promise<Map<string, Entity>>((resolve) => {
    getModel().then((model) => {
      const entities = new Map<string, Entity>();
      lineReader.on('line', (line) => {
        const entity = new Entity(model, JSON.parse(line) as IEntityDatum)
        entities.set(entity.id, entity);
      });
      lineReader.on('close', () => {
        resolve(entities);
      });
    });
  });
  return promise;
}

export async function getEntityById(id: string): Promise<Entity | undefined> {
  const entities = await getEntityMap();
  return entities.get(id)
}

export async function getEntityIds(): Promise<Array<string>> {
  const entities = await getEntityMap();
  return Array.from(entities.keys())
}