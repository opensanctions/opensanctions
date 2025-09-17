"use server"

import { Col, Form, Row, Table } from "react-bootstrap";

import { getPositions } from "@/lib/db";
import { Position } from "@/lib/db";
import { getCountries } from "@/lib/ftm";
import { ServerSearchParams } from "@/lib/pageProps";

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

  const positions = await getPositions(searchParams);

  return <>
    <Form className="mb-4" method="get">
      <Row>
        <Col md={6}>
          <input
            name="q"
            type="text"
            defaultValue={searchParams.q || ""}
            placeholder="Search position name"
            className="form-control"

          />
        </Col>
        <Col md={3}>
          <Form.Select name="country" defaultValue={searchParams.country || ""}>
            <option value="">Any country</option>
            {Array.from(countries.entries()).map(([code, name]) => {
              return <option key={code} value={code}>{name}</option>
            })}
          </Form.Select>
        </Col>
        <Col md={3}>
          <Form.Select name="is_pep" defaultValue={searchParams.is_pep || ""}>
            <option value="">Any PEP status</option>
            <option value="true">PEP</option>
            <option value="false">Not PEP</option>
            <option value="null">Undecided</option>
          </Form.Select>
        </Col>
      </Row>
    </Form>
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