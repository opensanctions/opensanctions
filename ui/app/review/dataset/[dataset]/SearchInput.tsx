'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState, useTransition } from 'react';

export default function SearchInput({ dataset }: { dataset: string }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [searchValue, setSearchValue] = useState(searchParams.get('q') || '');

  useEffect(() => {
    setSearchValue(searchParams.get('q') || '');
  }, [searchParams]);

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
