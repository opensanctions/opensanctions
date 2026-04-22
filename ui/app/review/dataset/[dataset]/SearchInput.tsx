'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { useState, useTransition } from 'react';

export default function SearchInput({ dataset }: { dataset: string }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const urlQuery = searchParams.get('q') || '';
  const [{ prevUrlQuery, searchValue }, setSearchState] = useState({
    prevUrlQuery: urlQuery,
    searchValue: urlQuery,
  });

  if (prevUrlQuery !== urlQuery) {
    setSearchState({ prevUrlQuery: urlQuery, searchValue: urlQuery });
  }

  const setSearchValue = (value: string) =>
    setSearchState({ prevUrlQuery, searchValue: value });

  const performSearch = (value: string) => {
    const params = new URLSearchParams(searchParams);
    if (value.trim()) {
      params.set('q', value.trim());
    } else {
      params.delete('q');
    }

    startTransition(() => {
      router.push(`/review/dataset/${encodeURIComponent(dataset)}?${params.toString()}`);
    });
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      performSearch(searchValue);
    }
  };

  return (
    <div className="mb-3">
      <input
        type="text"
        className="form-control"
        placeholder="Search reviews (source value, source URL, original extraction, extracted data, modified by)... Press Enter to search"
        value={searchValue}
        onChange={(e) => setSearchValue(e.target.value)}
        onKeyDown={handleKeyDown}
        disabled={isPending}
      />
    </div>
  );
}
