import Link from 'next/link';
import { getExtractionEntries } from '../../../lib/db';
import { verify } from '../../../lib/auth';
import { headers } from 'next/headers';
import { notFound } from 'next/navigation';
import { PageProps } from '@/lib/pageProps';

export default async function DatasetPage({ params }: PageProps) {
  // Check authorization
  const headersList = await headers();
  const email = await verify(headersList);

  if (!email) {
    return (
      <div className="container-fluid vh-100 d-flex flex-column justify-content-center align-items-center p-4 bg-light">
        <div className="text-center">
          <h1 className="text-danger mb-4">Access Denied</h1>
          <p className="lead mb-3">You are not authorized to access this application.</p>
          <p className="text-muted">Please contact your administrator if you believe this is an error.</p>
        </div>
      </div>
    );
  }

  const awaitedParams = await params;
  const entries = await getExtractionEntries(awaitedParams.dataset);
  if (!entries) return notFound();

  return (
    <div className="container-fluid  p-4 bg-light">
      <nav aria-label="breadcrumb" className="mb-3">
        <ol className="breadcrumb">
          <li className="breadcrumb-item">
            <Link href="/">Home</Link>
          </li>
          <li className="breadcrumb-item active" aria-current="page">
            {awaitedParams.dataset}
          </li>
        </ol>
      </nav>
      <h1 className="text-2xl font-bold mb-4">Reviews</h1>
      <div className="table-responsive-">
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
            {entries.map((entry: any) => (
              <tr key={entry.id}>
                <td>
                  <Link href={`/dataset/${encodeURIComponent(awaitedParams.dataset)}/${encodeURIComponent(entry.key)}`} className="text-primary text-decoration-underline">
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
