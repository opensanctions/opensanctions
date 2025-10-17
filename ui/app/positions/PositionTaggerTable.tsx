"use client"

import { useState, useEffect, useRef } from "react";
import { Table } from "react-bootstrap";

import { Position } from "@/lib/db";

import PositionTaggerRow from "./PositionTaggerRow";

type PositionTaggerTableProps = {
  countries: Map<string, string>;
  positions: Position[];
}

export default function PositionTaggerTable({ countries, positions }: PositionTaggerTableProps) {
  const [selectedEntityId, setSelectedEntityId] = useState<string | null>(null);
  const rowRefs = useRef<Map<string, HTMLTableRowElement>>(new Map());

  // Select first row by default when positions load
  useEffect(() => {
    if (positions.length > 0 && !selectedEntityId) {
      setSelectedEntityId(positions[0].entity_id);
    }
  }, [positions, selectedEntityId]);

  const handleRowSelect = (entityId: string) => {
    setSelectedEntityId(prev => prev === entityId ? null : entityId);
  };

  // Global keyboard listener for arrow navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
      if (!selectedEntityId) return;

      // Don't interfere if user is typing in an input
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) {
        return;
      }

      e.preventDefault();

      const entityIds = positions.map(p => p.entity_id);
      const currentIndex = entityIds.indexOf(selectedEntityId);

      if (e.key === 'ArrowDown' && currentIndex < entityIds.length - 1) {
        const nextId = entityIds[currentIndex + 1];
        setSelectedEntityId(nextId);
        rowRefs.current.get(nextId)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      } else if (e.key === 'ArrowUp' && currentIndex > 0) {
        const prevId = entityIds[currentIndex - 1];
        setSelectedEntityId(prevId);
        rowRefs.current.get(prevId)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedEntityId, positions]);

  const setRowRef = (entityId: string, element: HTMLTableRowElement | null) => {
    if (element) {
      rowRefs.current.set(entityId, element);
    } else {
      rowRefs.current.delete(entityId);
    }
  };

  return (
    <Table bordered hover>
      <thead>
        <tr>
          <th>Position</th>
          <th>Country</th>
          <th>Dataset</th>
          <th className="text-nowrap">Is a PEP</th>
          <th>Categories</th>
          <th>First seen</th>
          <th>Modified</th>
        </tr>
      </thead>
      <tbody>
        {
          positions.length == 0 ?
            <tr><td colSpan={6}>No matching results</td></tr> :
            positions.map((row: Position) => {
              return <PositionTaggerRow
                countries={countries}
                key={row.entity_id}
                position={row}
                isSelected={selectedEntityId === row.entity_id}
                onSelect={handleRowSelect}
                setRef={setRowRef}
              />
            })
        }
      </tbody>
    </Table>
  );
}

