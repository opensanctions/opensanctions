import { Metadata } from "next";
import { Container } from "react-bootstrap";

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

      <PositionTagger searchParams={await searchParams || {}} />

    </Container>
  );
}
