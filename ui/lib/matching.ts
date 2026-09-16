import TurndownService from 'turndown';

/**
 * Utilities for flagging extracted values that don't occur verbatim in the source text.
 */

// Shared across calls; this component runs both server- and client-side (SSR), and
// turndown (unlike the browser DOMParser) has a Node-safe fallback HTML parser.
const turndownService = new TurndownService();

// Codes and enum-like values (e.g. "US", "M") are often intentionally normalized away
// from the source text, so short leaves are excluded to avoid noisy false flags.
const MIN_MATCH_LENGTH = 3;

function normalizeForMatch(value: string): string {
  return value.toLowerCase().replace(/\s+/g, ' ').trim();
}

/**
 * Recursively collects string leaves from a parsed JSON/YAML value tree.
 */
export function collectStringLeaves(data: unknown): string[] {
  const leaves: string[] = [];
  function walk(node: unknown) {
    if (typeof node === 'string') {
      if (node.trim().length >= MIN_MATCH_LENGTH) {
        leaves.push(node);
      }
    } else if (Array.isArray(node)) {
      node.forEach(walk);
    } else if (node !== null && typeof node === 'object') {
      Object.values(node).forEach(walk);
    }
  }
  walk(data);
  return leaves;
}

/**
 * Returns the searchable plain-text rendering of a source value, or null if the
 * source has no text to search against (e.g. an image).
 */
export function getSourceSearchText(sourceValue: string, sourceMimeType: string): string | null {
  if (sourceMimeType === 'image/png') {
    return null;
  }
  if (sourceMimeType === 'text/html') {
    return turndownService.turndown(sourceValue);
  }
  return sourceValue;
}

/**
 * Returns the distinct string leaves of `data` that don't occur verbatim
 * (case/whitespace-insensitive) anywhere in `sourceText`.
 */
export function findValuesNotInSource(data: unknown, sourceText: string | null): string[] {
  if (sourceText === null) return [];
  const normalizedSource = normalizeForMatch(sourceText);
  const missing = new Set<string>();
  for (const leaf of collectStringLeaves(data)) {
    if (!normalizedSource.includes(normalizeForMatch(leaf))) {
      missing.add(leaf);
    }
  }
  return Array.from(missing);
}
