"use client"

import { useActionState } from "react";
import { Button, ButtonGroup, Form, Spinner } from "react-bootstrap";

import { Position } from "@/lib/db";

import { updatePosition, toggleTopic, PositionState } from "./actions";

type TopicProps = {
  topic: string,
  label: string,
  initialState: boolean,
  toggleTopicAction: (topic: string) => Promise<boolean>,
}

function Topic({ topic, label, initialState, toggleTopicAction}: TopicProps) {
  // Use a server-side action to toggle the topic. This is a bit of a tradeoff: More state on the client
  // and a more general server action, or less state on the client and a very specific server action.
  // We choose the latter here to keep things simple on the client.
  const [state, formAction, pending] = useActionState<boolean, FormData>(
    toggleTopicAction.bind(null, topic),
    initialState,
  );
  return (
    <Button
      active={state}
      variant="outline-dark"
      className="btn-sm"
      disabled={pending}
      formAction={formAction}
      type="submit"
    >
      {label}
    </Button>
  );
}

type PositionTaggerRowProps = {
  countries: Map<string, string>,
  position: Position,
}

export default function PositionTaggerRow({ countries, position }: PositionTaggerRowProps) {
  const countryLabels = position.countries.map((code: string) => {
    return countries.get(code);
  })


  const [state, formAction, pending] = useActionState<PositionState, FormData>(
    updatePosition.bind(null, position.entity_id),
    {
      is_pep: position.is_pep,
      // topics get handled in a separate action, see above
    },
  );

  const topicsSet = new Set(position.topics);
  // Server action to toggle a topic on this position
  const boundToggleTopic = toggleTopic.bind(null, position.entity_id);

  return (
    <tr key={position.entity_id}>
      <td title={position.entity_id}>
        {pending && <Spinner animation="border" role="status" />}
        {position.caption}
        {position.entity_id.startsWith('Q') &&
          <a href={`https://www.wikidata.org/wiki/${position.entity_id}`} target="_blank" rel="noreferrer">[WD]</a>
        }
      </td>
      <td>{countryLabels.join(", ")}</td>
      <td>{position.dataset}</td>
      <td>
        <Form action={formAction}>
          <Form.Check
            name="is_pep"
            type="checkbox"
            checked={state.is_pep ?? false}
            onChange={(event: React.ChangeEvent<HTMLInputElement>) => {
              event.target.form?.requestSubmit();
            }}
            disabled={pending}
          />
          {state.is_pep == null && "Undecided"}
        </Form>
      </td>
      <td>
        {/* For whatever reason, all of these Buttons need to be wrapped in a Form for Button formAction to work */}
        <Form>
          <ButtonGroup aria-label="Scope" className="pb-1">
            {Object.entries({
              "gov.national": "Nat",
              "gov.state": "Subnat",
              "gov.muni": "Local",
              "gov.igo": "IGO",
            }).map(([topic, label]) => (
              <Topic key={topic} topic={topic} label={label} initialState={topicsSet.has(topic)} toggleTopicAction={boundToggleTopic} />
            ))}
          </ButtonGroup>
          <ButtonGroup aria-label="Role">
            {Object.entries({
              "gov.head": "Head",
              "gov.executive": "Exec",
              "gov.legislative": "Legis",
              "gov.judicial": "Juris",
              "gov.security": "Secur",
              "gov.financial": "Fin",
              "gov.soe": "SOE",
              "role.diplo": "Diplo",
              "pol.party": "Party",
            }).map(([topic, label]) => (
              <Topic key={topic} topic={topic} label={label} initialState={topicsSet.has(topic)} toggleTopicAction={boundToggleTopic} />
            ))}
          </ButtonGroup>
        </Form>
      </td>
      <td className="text-nowrap" title={position.created_at.toISOString()}>{position.created_at.toISOString().slice(0, 10)}</td>
    </tr>
  )
}