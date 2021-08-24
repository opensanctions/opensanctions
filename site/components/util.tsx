import filesize from 'filesize';
import { Badge } from 'react-bootstrap';
import { Link45deg } from 'react-bootstrap-icons';
import { markdownToHtml } from '../lib/util';

import styles from '../styles/util.module.scss';

type NumericProps = {
  value?: number | null
}

export function Numeric({ value }: NumericProps) {
  // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Intl/NumberFormat
  if (value === undefined || value === null) {
    return null;
  }
  const fmt = new Intl.NumberFormat('en-US');
  return <span>{fmt.format(value)}</span>;
}

type NumericBadgeProps = {
  value?: number | null
  bg?: string
  className?: string
}

export function NumericBadge({ value, bg, className }: NumericBadgeProps) {
  return <Badge bg={bg || 'secondary'} className={className}><Numeric value={value} /></Badge>;
}

type PluralProps = {
  value?: number | null
  one: string
  many: string
}

export function Plural({ value, one, many }: PluralProps) {
  return <><Numeric value={value} /> {value === 1 ? one : many}</>;
}


type FileSizeProps = {
  size: number
}


export function FileSize({ size }: FileSizeProps) {
  return <span>{filesize(size, { round: 0, standard: 'jedec', locale: 'en-US' })}</span>
}

type MarkdownProps = {
  markdown?: string | null
}

export function Markdown({ markdown }: MarkdownProps) {
  if (markdown === undefined || markdown === null) {
    return null;
  }
  const html = markdownToHtml(markdown);
  return <div className="text-body" dangerouslySetInnerHTML={{ __html: html }} />
}

type FormattedDateProps = {
  date?: string | null
}

export function FormattedDate({ date }: FormattedDateProps) {
  if (date === undefined || date === null) {
    return null;
  }
  const obj = new Date(date as string);
  const fmt = new Intl.DateTimeFormat('en-CA', { dateStyle: 'medium', timeStyle: 'short' });
  return <time dateTime={date}>{fmt.format(obj)}</time>
}

type SummaryProps = {
  summary?: string | null
}

export function Summary({ summary }: SummaryProps) {
  if (summary === undefined || summary === null) {
    return null;
  }
  return <p className={styles.summary}>{summary}</p>
}


function getHost(href: string): string {
  try {
    const url = new URL(href);
    return url.hostname;
  } catch (e) {
    return href;
  }
}

type URLLinkProps = {
  url?: string | null
  label?: string
  icon?: boolean
}


export function URLLink({ url, label, icon = true }: URLLinkProps) {
  if (url === undefined || url === null) {
    return !!label ? <>{label}</> : null;
  }
  const href = /^https?:\/\//i.test(url) ? url : `//${url}`;
  const host = getHost(href);
  const textLabel = label || host;
  return (
    <>
      {icon && (
        <a href={href} target="_blank" title={textLabel}>
          <Link45deg className="bsIcon" />
        </a>
      )}
      <a href={href} target="_blank" title={url}>{textLabel}</a>
    </>
  );
}