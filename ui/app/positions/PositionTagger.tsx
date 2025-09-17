"use server"

import { Table } from "react-bootstrap";

import { getPositions, getPositionsDatasets } from "@/lib/db";
import { Position } from "@/lib/db";
import { getCountries } from "@/lib/ftm";
import { ServerSearchParams } from "@/lib/pageProps";

import PositionTaggerForm from "./PositionTaggerForm";
import PositionTaggerRow from "./PositionTaggerRow";
import { ResponsePagination } from "./ReponsePagination";


export default async function PositionTagger({ searchParams }: { searchParams: ServerSearchParams }) {

  const unsortedCountries = await getCountries();
  const countries = new Map(Array.from(unsortedCountries.entries()).sort((a, b) => b[1] < a[1] ? 1 : -1));
  const datasets = await getPositionsDatasets();

  // Filter out empty string search parameters
  const filteredSearchParams = Object.fromEntries(
    Object.entries(searchParams).filter(([, value]) => value !== '')
  )

  const positions = await getPositions({
    q: filteredSearchParams.q as string,
    dataset: filteredSearchParams.dataset as string,
    country: filteredSearchParams.country as string,
    is_pep: filteredSearchParams.is_pep == "null" ? null : filteredSearchParams.is_pep == "true" ? true : false,
    limit: filteredSearchParams.limit ? parseInt(filteredSearchParams.limit as string) : undefined,
    offset: filteredSearchParams.offset ? parseInt(filteredSearchParams.offset as string) : undefined,
  });

  return <>
    <PositionTaggerForm searchParams={searchParams} countries={countries} datasets={datasets} />

    <Table bordered hover>
      <thead>
        <tr>
          <th>Position</th>
          <th>Country</th>
          <th>Dataset</th>
          <th className="text-nowrap">Is a PEP</th>
          <th>Categories</th>
          <th>First seen</th>
        </tr>
      </thead>
      <tbody>
        {
          positions.results.length == 0 ?
            <tr><td colSpan={6}>No matching results</td></tr> :
            positions.results.map((row: Position) => {
              return <PositionTaggerRow countries={countries} key={row.entity_id} position={row} />
            })
        }
      </tbody>
    </Table>
    <div className="d-flex justify-content-center">
      <ResponsePagination response={positions} searchParams={searchParams} />
    </div>

    <div className="mb-4">
      <h5>Keyboard Shortcuts</h5>
      <div className="row">
        <div className="col-md-4">
          <strong>PEP Status:</strong>
          <ul className="list-unstyled">
            <li><kbd>X</kbd> Toggle PEP status</li>
            <li><kbd>U</kbd> Mark PEP status undecided</li>
          </ul>
        </div>
        <div className="col-md-4">
          <strong>Scope:</strong>
          <ul className="list-unstyled">
            <li><kbd>1</kbd> National</li>
            <li><kbd>2</kbd> Subnational</li>
            <li><kbd>3</kbd> Local</li>
            <li><kbd>4</kbd> IGO</li>
          </ul>
        </div>
        <div className="col-md-4">
          <strong>Roles:</strong>
          <ul className="list-unstyled">
            <li><kbd>Q</kbd> Head</li>
            <li><kbd>W</kbd> Executive</li>
            <li><kbd>E</kbd> Legislative</li>
            <li><kbd>R</kbd> Judicial</li>
            <li><kbd>T</kbd> Security</li>
            <li><kbd>Y</kbd> Financial</li>
          </ul>
        </div>
      </div>
    </div>
  </>
}