import queryString from 'query-string';
import { Pagination } from "react-bootstrap";

import { ServerSearchParams } from "@/lib/pageProps"

export interface IPaginatedResponse {
  total: number
  limit: number
  offset: number
}

type ResponsePaginationProps = {
  response: IPaginatedResponse
  searchParams: ServerSearchParams
}

export async function ResponsePagination({ response, searchParams }: ResponsePaginationProps) {
  if (response.total === 0) {
    return null;
  }
  const nextOffset = response.offset + response.limit;
  const upper = Math.min(response.total, nextOffset);
  const hasPrev = response.offset > 0;
  const hasNext = response.total> nextOffset;

  const prevLink = queryString.stringify({
    ...searchParams,
    offset: Math.max(0, response.offset - response.limit)
  })
  const nextLink = queryString.stringify({
    ...searchParams,
    offset: response.offset + response.limit
  })
  //const relationText = response.total.relation == 'gte' ? 'more than ' : '';



  return (
    <Pagination>
      {/* TODO: Fix this */}
      {/* <Pagination.Prev disabled={!hasPrev} href={`?${prevLink}`} />
      <Pagination.Item disabled>
        {response.offset + 1} - {upper} of {response.total}
      </Pagination.Item>
      <Pagination.Next disabled={!hasNext} href={`?${nextLink}`} /> */}
    </Pagination>
  );
}