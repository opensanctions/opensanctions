"use server"

import { Table } from "react-bootstrap";

import { getPositions } from "@/lib/db";
import { Position } from "@/lib/db";
import { getCountries } from "@/lib/ftm";
import { ServerSearchParams } from "@/lib/pageProps";

import PositionTaggerForm from "./PositionTaggerForm";
import PositionTaggerRow from "./PositionTaggerRow";
import { ResponsePagination } from "./ReponsePagination";





// export async function getPositions(querystring: string): Promise<ITaggingSearchResponse | null> {
//   const apiUrl = `${API_URL}/positions/?${querystring}`;
//   const result = await fetchJsonUrl<ITaggingSearchResponse>(apiUrl);
//   return result;
// }



export default async function PositionTagger({ searchParams }: { searchParams: ServerSearchParams }) {

  const unsortedCountries = await getCountries();
  const countries = new Map(Array.from(unsortedCountries.entries()).sort((a, b) => b[1] < a[1] ? 1 : -1));

  // Filter out empty string search parameters
  const filteredSearchParams = Object.fromEntries(
    Object.entries(searchParams).filter(([, value]) => value !== '')
  );

  const positions = await getPositions(filteredSearchParams);

  return <>
    <PositionTaggerForm searchParams={searchParams} countries={countries} />
    <Table bordered hover>
      <thead>
        <tr>
          <th>Position</th>
          <th>Country</th>
          <th className="text-nowrap">Is a PEP</th>
          <th>Categories</th>
          <th>First seen</th>
        </tr>
      </thead>
      <tbody>
        {
          positions.results.length == 0 ?
            <tr><td colSpan={5}>No matching results</td></tr> :
            positions.results.map((row: Position) => {
              return <PositionTaggerRow countries={countries} key={row.entity_id} position={row} />
            })
        }
      </tbody>
    </Table>
    <ResponsePagination response={positions} searchParams={searchParams} />
  </>
}