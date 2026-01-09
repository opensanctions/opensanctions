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
      <nav aria-label="breadcrumb" className="mb-3">
        <ol className="breadcrumb">
          <li className="breadcrumb-item active" aria-current="page">
            Reviews
          </li>
        </ol>
      </nav>
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
