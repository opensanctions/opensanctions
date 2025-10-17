import { Metadata } from 'next';

import { getDatasetStats, IDatasetStats } from '@/lib/db';

export const dynamic = "force-dynamic";

const TITLE = "Reviews";

export const metadata: Metadata = {
  title: TITLE,
  description: "Review, correct and accept automated data extraction results.",
};


export default async function Home() {
  const stats = await getDatasetStats();

  return (
    <>
      <div className="d-flex justify-content-between align-items-center mb-4">
        <h1 className="text-2xl font-bold mb-0">{TITLE}</h1>
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
    </>
  );
}
