"use client"

import { Col, Form, Row } from "react-bootstrap";

import { ServerSearchParams } from "@/lib/pageProps";

interface PositionTaggerFormProps {
  searchParams: ServerSearchParams;
  countries: Map<string, string>;
  datasets: string[];
}

function submitForm(e: React.ChangeEvent<HTMLSelectElement>) {
  e.target.form?.submit();
}

export default function PositionTaggerForm({ searchParams, countries, datasets }: PositionTaggerFormProps) {
  return (
    <Form className="mb-4" method="get">
      <Row>
        <Col md={4}>
          <input
            name="q"
            type="text"
            defaultValue={searchParams.q || ""}
            placeholder="Search position name"
            className="form-control"
          />
        </Col>
        <Col md={2}>
          <Form.Select name="country" defaultValue={searchParams.country || ""} onChange={submitForm}>
            <option value="">Any country</option>
            {Array.from(countries.entries()).map(([code, name]) => {
              return <option key={code} value={code}>{name}</option>
            })}
          </Form.Select>
        </Col>
        <Col md={2}>
          <Form.Select name="dataset" defaultValue={searchParams.dataset || ""} onChange={submitForm}>
            <option value="">Any dataset</option>
            {datasets.map((dataset) => {
              return <option key={dataset} value={dataset}>{dataset}</option>
            })}
          </Form.Select>
        </Col>
        <Col md={2}>
          <Form.Select name="is_pep" defaultValue={searchParams.is_pep || ""} onChange={submitForm}>
            <option value="">Any PEP status</option>
            <option value="true">PEP</option>
            <option value="false">Not PEP</option>
            <option value="null">Undecided</option>
          </Form.Select>
        </Col>
      </Row>
    </Form>
  );
}
