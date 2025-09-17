"use client"

import { useState } from "react";
import { Button, ButtonGroup, Form, Spinner } from "react-bootstrap";

import { Position, PositionUpdate } from "@/lib/db";

type TopicProps = {
  label: string
  topic: string
  topics: Array<string>
  disabled: boolean
  onClick: (event: React.MouseEvent<HTMLButtonElement, MouseEvent>) => void
}

function Topic({ label, topic, topics, disabled, onClick }: TopicProps) {
  return <Button
    name="topic"
    value={topic}
    variant="outline-dark"
    className="btn-sm"
    active={topics.includes(topic)}
    disabled={disabled}
    onClick={onClick}
  >
    {label}
  </Button>
}

type PositionTaggerRowProps = {
  countries: Map<string, string>,
  position: Position,
}

export default function PositionTaggerRow({ countries, position }: PositionTaggerRowProps) {
  // We JSON-ize the position data once to avoid having to deal with differences between
  // initial rendering (however that work) and the JSON returned by the update request.
  const [positionData, setPositionData] = useState(JSON.parse(JSON.stringify(position)));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(false);

  const countryLabels = positionData.countries.map((code: string) => {
    return countries.get(code);
  })

  const onPositionEdit = async (event: React.ChangeEvent<HTMLInputElement>) => {
    updatePosition({ is_pep: event.target.checked });
  }

  const onTopicEdit = async (event: React.MouseEvent<HTMLButtonElement, MouseEvent>) => {
    const target = event.target as HTMLButtonElement;
    const updatedTopics = [...positionData.topics];
    const index = updatedTopics.indexOf(target.value);
    if (index == -1) {
      updatedTopics.push(target.value);
    } else {
      updatedTopics.splice(index, 1);
    }
    updatePosition({ topics: updatedTopics });
  }

  const updatePosition = (partialData: Partial<PositionUpdate>) => {
    setSaving(true);
    const url = `/api/positions/${positionData.entity_id}`;
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
          setPositionData(result);
          setSaving(false);
        }
      })
      .catch(() => {
        setError(true);
        setSaving(false);
      });
  }

  const topics = positionData.topics;
  return (
    <tr key={positionData.entity_id}>
      <td title={positionData.entity_id}>
        {saving && <Spinner animation="border" role="status" />}
        {positionData.caption}
        {positionData.entity_id.startsWith('Q') &&
          <a href={`https://www.wikidata.org/wiki/${positionData.entity_id}`} target="_blank" rel="noreferrer">[WD]</a>
        }
        {error && <span className="text-danger">Error saving</span>}
      </td>
      <td>{countryLabels.join(", ")}</td>
      <td>
        <Form.Check
          name="is_pep"
          type="checkbox"
          checked={positionData.is_pep ?? false}
          onChange={onPositionEdit}
          disabled={saving}
        />
        {positionData.is_pep == null && "Undecided"}
      </td>
      <td>
        <ButtonGroup aria-label="Scope" className="pb-1 d-inline-block">
          <Topic label="Nat" topics={topics} topic="gov.national" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Subnat" topics={topics} topic="gov.state" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Local" topics={topics} topic="gov.muni" disabled={saving} onClick={onTopicEdit} />
          <Topic label="IGO" topics={topics} topic="gov.igo" disabled={saving} onClick={onTopicEdit} />
        </ButtonGroup>

        <ButtonGroup aria-label="Role">
          <Topic label="Head" topics={topics} topic="gov.head" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Exec" topics={topics} topic="gov.executive" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Legis" topics={topics} topic="gov.legislative" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Juris" topics={topics} topic="gov.judicial" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Secur" topics={topics} topic="gov.security" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Fin" topics={topics} topic="gov.financial" disabled={saving} onClick={onTopicEdit} />
          <Topic label="SOE" topics={topics} topic="gov.soe" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Diplo" topics={topics} topic="role.diplo" disabled={saving} onClick={onTopicEdit} />
          <Topic label="Party" topics={topics} topic="pol.party" disabled={saving} onClick={onTopicEdit} />
        </ButtonGroup>
      </td>
      <td className="text-nowrap" title={positionData.created_at}>{positionData.created_at.slice(0, 10)}</td>
    </tr>
  )
}