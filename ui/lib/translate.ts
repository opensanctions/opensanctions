import { syntaxTree } from '@codemirror/language';
import { EditorView, hoverTooltip, TooltipView } from '@codemirror/view';

// Scripts a reviewer plausibly can't read and might want translated:
// Greek through Thaana (incl. Cyrillic, Armenian, Hebrew, Arabic, Syriac),
// Indic scripts through Hangul Jamo (incl. Thai, Lao, Tibetan, Myanmar,
// Georgian), Khmer, kana, Hangul compatibility jamo, CJK, Hangul syllables.
const NON_LATIN_SCRIPT = new RegExp(
  '[\\u0370-\\u07BF\\u0900-\\u11FF\\u1780-\\u17FF\\u3040-\\u30FF\\u3130-\\u318F' +
  '\\u3400-\\u4DBF\\u4E00-\\u9FFF\\uAC00-\\uD7AF\\uF900-\\uFAFF]'
);

// Syntax node types treated as primitive string values in the YAML/JSON trees.
const VALUE_NODE_TYPES = ['String', 'Literal', 'QuotedLiteral', 'BlockLiteral'];

// Matches the server-side limit in app/api/translate/route.ts
const MAX_TEXT_LENGTH = 3000;

type TranslationResult = { translation: string; sourceLanguage: string };

// Session-level cache so re-hovering a value doesn't re-query the API.
const translationCache = new Map<string, TranslationResult>();

function stripQuotes(text: string): string {
  return text.replace(/^["']|["']$/g, '');
}

/**
 * Determine the value to translate at the hovered position: a primitive
 * string value from the syntax tree if there is one, otherwise the hovered
 * line (for plain text / raw HTML views without a useful tree).
 */
function hoverTarget(view: EditorView, pos: number): { from: number; to: number; text: string } | null {
  const tree = syntaxTree(view.state);
  const node = tree?.resolveInner(pos, 0);
  if (node !== undefined && VALUE_NODE_TYPES.includes(node.type.name)) {
    const text = stripQuotes(view.state.doc.sliceString(node.from, node.to)).trim();
    return { from: node.from, to: node.to, text };
  }
  const line = view.state.doc.lineAt(pos);
  return { from: line.from, to: line.to, text: line.text.trim() };
}

async function requestTranslation(text: string): Promise<TranslationResult> {
  const response = await fetch('/api/translate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(typeof data.error === 'string' ? data.error : 'Translation failed');
  }
  if (typeof data.translation !== 'string' || typeof data.sourceLanguage !== 'string') {
    throw new Error('Translation returned an invalid result');
  }
  return { translation: data.translation, sourceLanguage: data.sourceLanguage };
}

// All content is inserted via textContent — never innerHTML — so nothing in
// the source value or the model response can inject markup or script.
function renderResult(dom: HTMLElement, result: TranslationResult) {
  dom.replaceChildren();
  const translation = document.createElement('div');
  translation.textContent = result.translation;
  dom.appendChild(translation);
  const language = document.createElement('div');
  language.textContent = `Translated from ${result.sourceLanguage}`;
  language.className = 'text-muted';
  language.style.fontSize = '0.85em';
  language.style.marginTop = '2px';
  dom.appendChild(language);
}

function renderError(dom: HTMLElement, message: string) {
  dom.replaceChildren();
  const error = document.createElement('div');
  error.textContent = message;
  error.className = 'text-danger';
  dom.appendChild(error);
}

function renderButton(dom: HTMLElement, text: string) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'btn btn-sm btn-outline-secondary';
  button.textContent = 'Translate';
  button.addEventListener('click', async () => {
    button.disabled = true;
    button.textContent = 'Translating…';
    try {
      const result = await requestTranslation(text);
      translationCache.set(text, result);
      renderResult(dom, result);
    } catch (e: unknown) {
      renderError(dom, e instanceof Error ? e.message : 'Translation failed');
    }
  });
  dom.replaceChildren(button);
}

function buildTooltip(text: string): TooltipView {
  const dom = document.createElement('div');
  dom.className = 'cm-translation-tooltip';
  dom.setAttribute('aria-live', 'polite');
  dom.style.padding = '6px 8px';
  dom.style.maxWidth = '36em';
  dom.style.fontFamily = 'var(--bs-body-font-family, sans-serif)';
  const cached = translationCache.get(text);
  if (cached !== undefined) {
    renderResult(dom, cached);
  } else {
    renderButton(dom, text);
  }
  return { dom };
}

/**
 * CodeMirror extension: hovering a value written in a non-Latin script
 * offers an English translation in a tooltip, leaving the surrounding
 * values visible for comparison.
 */
export const translationTooltip = hoverTooltip((view, pos) => {
  const target = hoverTarget(view, pos);
  if (target === null) return null;
  const { from, to, text } = target;
  if (text.length === 0 || text.length > MAX_TEXT_LENGTH) return null;
  if (!NON_LATIN_SCRIPT.test(text)) return null;
  return {
    pos: from,
    end: to,
    above: true,
    create: () => buildTooltip(text),
  };
});
