"use client"

import Link from "next/link";
import { useState, useEffect, useRef, useCallback } from "react";
import { Button, ButtonGroup, Form, Spinner } from "react-bootstrap";
import { BoxArrowUpRight } from "react-bootstrap-icons";

import { OPENSANCTIONS_WEBSITE_BASE_URL } from "@/lib/constants";
import { Position, PositionUpdate } from "@/lib/db";

const SCOPE_ENTRIES = [
  ["gov.national", "Nat"],
  ["gov.state", "Subnat"],
  ["gov.muni", "Local"],
  ["gov.igo", "IGO"]
] as const;

const ROLE_ENTRIES = [
  ["gov.head", "Head"],
  ["gov.executive", "Exec"],
  ["gov.legislative", "Legis"],
  ["gov.judicial", "Juris"],
  ["gov.security", "Secur"],
  ["gov.financial", "Fin"],
  ["gov.soe", "SOE"],
  ["role.diplo", "Diplo"],
  ["pol.party", "Party"],
  ["gov.religion", "Relig"]
] as const;

type PositionTaggerRowProps = {
  countries: Map<string, string>,
  position: Position,
}

export default function PositionTaggerRow({ countries, position }: PositionTaggerRowProps) {
  const countryLabels = position.countries.map((code: string) => {
    return countries.get(code);
  })

  // Initial state for the component
  const [state, setState] = useState({
    is_pep: position.is_pep,
    topics: new Set(position.topics),
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const rowRef = useRef<HTMLTableRowElement>(null);

  const updatePosition = useCallback((partialData: Partial<PositionUpdate>) => {
    setSaving(true);
    const url = `/api/positions/${position.entity_id}`;
    fetch(url, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(partialData),
    })
      .then(response => response.json())
      .then((result) => {
        if (result == null) {
          setError(true);
        } else {
          setState(prevState => ({
            ...prevState,
            is_pep: result.is_pep,
            topics: new Set(result.topics)
          }));
          setSaving(false);
        }
      })
      .catch(() => {
        setError(true);
        setSaving(false);
      });
  }, [position.entity_id]);

  const toggleTopic = useCallback((topic: string) => {
    const updatedTopics = [...state.topics];
    if (updatedTopics.includes(topic)) {
      updatedTopics.splice(updatedTopics.indexOf(topic), 1);
    } else {
      updatedTopics.push(topic);
    }
    updatePosition({ topics: updatedTopics });
  }, [state.topics, updatePosition]);

  // Keyboard shortcuts for when row is hovered
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (!isHovered || saving) return;
      // If any modifier key is pressed, let the browser handle it
      if (e.metaKey || e.ctrlKey || e.altKey) {
        return; // Don't handle the key, let browser shortcuts work
      }

      if (e.key === 'x') {
        e.preventDefault();
        updatePosition({ is_pep: !state.is_pep });
      } else if (e.key === 'u') {
        e.preventDefault();
        updatePosition({ is_pep: null });
      } else if (e.key === '1') {
        e.preventDefault();
        toggleTopic(SCOPE_ENTRIES[0][0]);
      } else if (e.key === '2') {
        e.preventDefault();
        toggleTopic(SCOPE_ENTRIES[1][0]);
      } else if (e.key === '3') {
        e.preventDefault();
        toggleTopic(SCOPE_ENTRIES[2][0]);
      } else if (e.key === '4') {
        e.preventDefault();
        toggleTopic(SCOPE_ENTRIES[3][0]);
      } else if (e.key === 'q') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[0][0]);
      } else if (e.key === 'w') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[1][0]);
      } else if (e.key === 'e') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[2][0]);
      } else if (e.key === 'r') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[3][0]);
      } else if (e.key === 't') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[4][0]);
      } else if (e.key === 'y') {
        e.preventDefault();
        toggleTopic(ROLE_ENTRIES[5][0]);
      }
    }

    if (isHovered) {
      window.addEventListener('keydown', handleKeyDown);
      return () => window.removeEventListener('keydown', handleKeyDown);
    }
  }, [isHovered, saving, state.is_pep, updatePosition, toggleTopic]);

  return (
    <tr
      key={position.entity_id}
      ref={rowRef}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <td title={position.entity_id}>
        {saving && <Spinner animation="border" role="status" />}
        {position.caption}
        {position.entity_id.startsWith('Q') &&
          <a href={`https://www.wikidata.org/wiki/${position.entity_id}`} target="_blank" rel="noreferrer">[WD]</a>
        }
        <small><Link className="ps-2 align-text-bottom" href={`${OPENSANCTIONS_WEBSITE_BASE_URL}/entities/${position.entity_id}`} target="_blank" rel="noreferrer"><BoxArrowUpRight /></Link></small>
        {error && <span className="text-danger">Error saving</span>}
      </td>
      <td>{countryLabels.join(", ")}</td>
      <td>{position.dataset}</td>
      <td>
        <Form.Check
          name="is_pep"
          type="checkbox"
          checked={state.is_pep ?? false}
          onChange={() => updatePosition({ is_pep: !state.is_pep })}
          disabled={saving}
        />
        {state.is_pep == null && "Undecided"}
      </td>
      <td>
        {/* Scope Buttons */}
        <div>
          <ButtonGroup aria-label="Scope" >
            {SCOPE_ENTRIES.map(([topic, label]) => (
              <Button
                key={topic}
                variant="outline-dark"
                className="btn-sm"
                active={state.topics.has(topic)}
                disabled={saving}
                onClick={() => toggleTopic(topic)}
              >
                {label}
              </Button>
            ))}
          </ButtonGroup>
        </div>

        {/* Role Buttons */}
        <div>
          <ButtonGroup aria-label="Role" className="pt-1">
            {ROLE_ENTRIES.map(([topic, label]) => (
              <Button
                key={topic}
                variant="outline-dark"
                className="btn-sm"
                active={state.topics.has(topic)}
                disabled={saving}
                onClick={() => toggleTopic(topic)}
              >
                {label}
              </Button>
            ))}
          </ButtonGroup>
        </div>
      </td>
      <td className="text-nowrap" title={position.created_at.toISOString()}>{position.created_at.toISOString().slice(0, 10)}</td>
    </tr>
  )
}