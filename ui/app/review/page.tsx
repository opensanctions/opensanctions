import Head from 'next/head';
import { headers } from 'next/headers';

import { verify } from '@/lib/auth';
import { getDatasetStats, IDatasetStats } from '@/lib/db';

export const dynamic = "force-dynamic";

export default async function Home() {
  // Check authorization
  const headersList = await headers();
  const email = await verify(headersList);

  const stats = await getDatasetStats();

  return (
    <>
      <Head>
        <title>Zavod Reviews</title>
      </Head>
      <div className="container-fluid d-flex flex-column p-4">
        <div className="d-flex justify-content-between align-items-center mb-4">
          <h1 className="text-2xl font-bold mb-0">Zavod Reviews</h1>
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
            {stats.map((row: IDatasetStats) => (
              <tr key={row.dataset}>
                <td>
                  <a href={`/review/dataset/${encodeURIComponent(row.dataset)}`} className="text-primary text-decoration-underline">
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
