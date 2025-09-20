"use server";
import { verify } from "@/lib/auth";
import { getPositionByEntityId, Position, PositionUpdate, softDeleteAndCreatePosition } from "@/lib/db";
import { headers } from "next/headers";

// export async function getPositions(querystring: string): Promise<ITaggingSearchResponse | null> {
//   const apiUrl = `${API_URL}/positions/?${querystring}`;
//   const result = await fetchJsonUrl<ITaggingSearchResponse>(apiUrl);
//   return result;
// }
export interface PositionState {
  is_pep: boolean | null;
}

export async function updatePosition(entityId: string, prevState: PositionState, formData: FormData) {
  const email = await verify(await headers());

  const positionUpdate: PositionUpdate = {}
  if (formData.get('is_pep') !== null) {
    positionUpdate.is_pep = formData.get('is_pep') === 'on';
  }

  // We're quite aggresive here, throwing lots of errors if something goes wrong
  // but not caching them. Oh well, this is internal, so 500 or 404 doesn't matter
  // all that much.
  const updatedPosition = await softDeleteAndCreatePosition({
    entityId: entityId,
    positionUpdate: positionUpdate,
    modifiedBy: email!,
  });

  return {
    is_pep: updatedPosition.is_pep,
  }
}

// Server action to toggle a topic on a position. We do this because it's easier on the client to just
// attach an action to a button than to maintain (and update) the state of the topics on the client.
export async function toggleTopic(entityId: string, topic: string): Promise<boolean> {
  const email = await verify(await headers());
  const position: Position = await getPositionByEntityId(entityId);
  const topicsSet = new Set(position.topics);
  if (topicsSet.has(topic)) {
    topicsSet.delete(topic);
  } else {
    topicsSet.add(topic);
  }
  await softDeleteAndCreatePosition({
    entityId: entityId,
    positionUpdate: {
      topics: [...topicsSet],
    },
    modifiedBy: email!,
  });

  return topicsSet.has(topic);
}

