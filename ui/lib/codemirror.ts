import { Decoration, DecorationSet, EditorView, MatchDecorator, ViewPlugin, ViewUpdate } from "@codemirror/view";

// Not using the builtin RegExp.escape: unlike createHighlighter's use of it (only ever
// reached client-side, from user-triggered search state that's empty on first render),
// this is reached during SSR too, on a Node version that doesn't implement it yet.
function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Creates a CodeMirror ViewPlugin that highlights all matches of a search query.
 *
 * @param searchQuery - The text to search for and highlight (case-insensitive)
 * @returns A ViewPlugin for highlighting matches, or empty array if no query provided
 */
export function createHighlighter(searchQuery: string) {
  if (!searchQuery) return [];
  const decorator = new MatchDecorator({
    regexp: new RegExp(RegExp.escape(searchQuery.toLowerCase()), 'gi'),
    decoration: Decoration.mark({ class: 'cm-searchMatch' })
  });

  return ViewPlugin.fromClass(class {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = decorator.createDeco(view);
    }

    update(update: ViewUpdate) {
      this.decorations = decorator.updateDeco(update, this.decorations);
    }
  }, {
    decorations: v => v.decorations
  });
}

/**
 * Creates a CodeMirror ViewPlugin that flags occurrences of extracted values
 * which don't occur verbatim in the source (see lib/matching.ts).
 *
 * @param missingValues - The literal extracted-value strings to flag
 * @returns A ViewPlugin for flagging matches, or empty array if none provided
 */
export function createMissingValueHighlighter(missingValues: string[]) {
  if (missingValues.length === 0) return [];
  // Longest first, so a value that's a substring of another is not matched partially.
  const pattern = missingValues
    .slice()
    .sort((a, b) => b.length - a.length)
    .map(value => escapeRegExp(value))
    .join('|');
  const decorator = new MatchDecorator({
    regexp: new RegExp(pattern, 'g'),
    decoration: Decoration.mark({
      class: 'cm-missingSource',
      attributes: { title: 'Not found verbatim in source' }
    })
  });

  return ViewPlugin.fromClass(class {
    decorations: DecorationSet;

    constructor(view: EditorView) {
      this.decorations = decorator.createDeco(view);
    }

    update(update: ViewUpdate) {
      this.decorations = decorator.updateDeco(update, this.decorations);
    }
  }, {
    decorations: v => v.decorations
  });
}
