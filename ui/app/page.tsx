import { getDatasetStats } from '../lib/db';
import { verify } from '../lib/auth';
import { headers } from 'next/headers';
import Head from 'next/head';

export const dynamic = "force-dynamic";

export default async function Home() {
  // Check authorization
  const headersList = await headers();
  const email = await verify(headersList);

  if (!email) {
    return (
      <>
        <Head>
          <title>Unauthorized - Zavod Reviews</title>
        </Head>
        <div className="container-fluid vh-100 d-flex flex-column justify-content-center align-items-center p-4 bg-light">
          <div className="text-center">
            <h1 className="text-danger mb-4">Access Denied</h1>
            <p className="lead mb-3">You are not authorized to access this application.</p>
            <p className="text-muted">Please contact your administrator if you believe this is an error.</p>
          </div>
        </div>
      </>
    );
  }

  const stats = await getDatasetStats();

  return (
    <>
      <Head>
        <title>Zavod Reviews</title>
      </Head>
      <div className="container-fluid vh-100 d-flex flex-column p-4 bg-light">
        <div className="d-flex justify-content-between align-items-center mb-4">
          <h1 className="text-2xl font-bold mb-0">Zavod Reviews</h1>
          <div className="text-muted">
            Welcome, <strong>{email}</strong>
          </div>
        </div>
        <p className="mb-3">Unaccepted reviews are pending human verification. Only reviews for the latest version of each dataset are shown.</p>
        <table className="table table-bordered w-100">
          <thead>
            <tr>
              <th>Dataset</th>
              <th>Total Reviews</th>
              <th>Unaccepted Reviews</th>
            </tr>
          </thead>
          <tbody>
            {stats.map((row: any) => (
              <tr key={row.dataset}>
                <td>
                  <a href={`/dataset/${encodeURIComponent(row.dataset)}`} className="text-primary text-decoration-underline">
                    {row.dataset}
                  </a>
                </td>
                <td>{row.total}</td>
                <td>{row.unaccepted}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
