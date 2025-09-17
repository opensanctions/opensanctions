import { Model, IModelDatum } from "@opensanctions/followthemoney";
import { unstable_cache } from "next/cache";


const FTM_MODEL_URL = "https://data.opensanctions.org/meta/model.json";

interface IModelSpec {
  app: string
  version: string
  model: IModelDatum
  target_topics: string[]
  // We're not using this ye
  //matcher: { [key: string]: IMatcherFeature }
}

export const getModel = unstable_cache(async () => {
    const response = await fetch(FTM_MODEL_URL, { cache: "force-cache" });
    const modelSpec = await response.json() as any as IModelSpec;
    return new Model(modelSpec.model);
})

export async function getCountries(): Promise<Map<string, string>> {
    const ftmModel = await getModel();
    return ftmModel.getType('country').values;
}