import { Decoration, DecorationSet, EditorView, MatchDecorator, ViewPlugin, ViewUpdate } from "@codemirror/view";

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
