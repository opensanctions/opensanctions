import { getDatasetStats } from '../lib/db';
import Head from 'next/head';

export const dynamic = "force-dynamic";

export default async function Home() {
  const stats = await getDatasetStats();

  return (
    <>
      <Head>
        <title>Zavod Extraction</title>
      </Head>
      <div className="container-fluid vh-100 d-flex flex-column p-4 bg-light">
        <h1 className="text-2xl font-bold mb-4">Zavod Extraction</h1>
        <p className="mb-3">Unaccepted items are pending human verification.</p>
        <table className="table table-bordered w-100">
          <thead>
            <tr>
              <th>Dataset</th>
              <th>Total Items</th>
              <th>Unaccepted Items</th>
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
