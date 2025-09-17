import { Metadata } from "next";
import { Col, Container, Row } from "react-bootstrap";

import { PageProps } from "@/lib/pageProps";

import PositionTagger from "./PositionTagger";


export const dynamic = 'force-dynamic';
const TITLE = 'PEP position administration';
const SUMMARY = "Maintain categorisation of PEP positions"

export const metadata: Metadata = {
  title: TITLE,
  description: SUMMARY,
}

export default async function Page({searchParams}: PageProps) {
  return (
    <Container>
      <h1>{TITLE}</h1>
      <Row>
        <Col md={9}>
          <PositionTagger searchParams={await searchParams || {}} />
        </Col>
      </Row>
    </Container>
  );
}
