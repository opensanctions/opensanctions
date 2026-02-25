import Link from 'next/link';
import { notFound } from 'next/navigation';

import { getExtractionEntries } from '@/lib/db';
import { PageProps } from '@/lib/pageProps';
import SearchInput from './SearchInput';


export default async function DatasetPage({ params, searchParams }: PageProps) {
  const awaitedParams = await params;
  const awaitedSearchParams = await searchParams;
  const searchQuery = awaitedSearchParams?.q as string | undefined;
  const entries = await getExtractionEntries(awaitedParams.dataset, searchQuery);
  if (!entries) return notFound();

  return (
    <div>
      <nav aria-label="breadcrumb" className="mb-3">
        <ol className="breadcrumb">
          <li className="breadcrumb-item">
            <Link href="/review">Reviews</Link>
          </li>
          <li className="breadcrumb-item active" aria-current="page">
            {awaitedParams.dataset}
          </li>
        </ol>
      </nav>
      <SearchInput dataset={awaitedParams.dataset} />
      <div className="">
        <table className="table table-bordered ">
          <thead>
            <tr>
              <th>Key</th>
              <th>Accepted</th>
              <th>Modified At</th>
              <th>Modified By</th>
              <th>Raw Data</th>
              <th>Source URL</th>
            </tr>
          </thead>
          <tbody>
            {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
            {entries.map((entry: any) => (
              <tr key={entry.id}>
                <td>
                  <Link href={`/review/dataset/${encodeURIComponent(awaitedParams.dataset)}/${encodeURIComponent(entry.key)}`} className="text-primary text-decoration-underline">
                    {entry.key.length > 23
                      ? `${entry.key.slice(0, 10)}...${entry.key.slice(-10)}`
                      : entry.key}
                  </Link>
                </td>
                <td>{entry.accepted ? 'Yes' : 'No'}</td>
                <td>{entry.modified_at ? new Date(entry.modified_at).toLocaleString() : ''}</td>
                <td>{entry.modified_by || ''}</td>
                <td title={entry.raw_data_snippet}>{entry.raw_data_snippet}</td>
                <td>
                  <a href={entry.source_url} className="text-primary text-decoration-underline" target="_blank" rel="noopener noreferrer">
                    {entry.source_url && entry.source_url.length > 43
                      ? `${entry.source_url.slice(0, 20)}...${entry.source_url.slice(-20)}`
                      : entry.source_url}
                  </a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
