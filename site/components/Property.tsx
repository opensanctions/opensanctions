import { ComponentType } from 'react';

import { Property, PropertyType } from "@alephdata/followthemoney";
import { OpenSanctionsEntity, Value, Values } from "../lib/types";
import { FormattedDate, SpacedList, URLLink } from "./util";
import { EntityLink, EntityProps } from "./Entity";


type TypeValueProps = {
  type: PropertyType
  value: Value
  prop?: Property
  entity?: ComponentType<EntityProps>
}

export function TypeValue({ type, value, entity: Entity = EntityLink, prop }: TypeValueProps) {
  if (['country', 'language', 'topic'].indexOf(type.name) != -1) {
    return <>{type.values.get(value as string) || value}</>
  }
  if (type.name === 'date') {
    return <FormattedDate date={value as string} />
  }
  if (type.name === 'url') {
    return <URLLink url={value as string} />
  }
  if (type.name === 'identifier') {
    return <code>{value}</code>
  }
  if (type.name === 'entity') {
    if (typeof (value) !== 'string') {
      return <Entity entity={value as OpenSanctionsEntity} via={prop} />
    }
    return <code>{value}</code>
  }
  return <>{value}</>
}

type TypeValuesProps = {
  type: PropertyType
  values: Values
  prop?: Property
  entity?: ComponentType<EntityProps>
}

export function TypeValues({ type, values, entity, prop }: TypeValuesProps) {
  const elems = values.sort().map((v) => <TypeValue type={type} value={v} entity={entity} prop={prop} />)
  if (elems.length === 0) {
    return <span className="text-muted">not available</span>
  }
  return <SpacedList values={elems} />
}

type PropertyValueProps = {
  prop: Property
  value: Value
  entity?: ComponentType<EntityProps>
}

export function PropertyValue({ prop, value, entity }: PropertyValueProps) {
  return <TypeValue type={prop.type} value={value} entity={entity} prop={prop} />
}

type PropertyValuesProps = {
  prop: Property
  values: Values
  entity?: ComponentType<EntityProps>
}

export function PropertyValues({ prop, values, entity }: PropertyValuesProps) {
  return <TypeValues type={prop.type} values={values} entity={entity} prop={prop} />
}