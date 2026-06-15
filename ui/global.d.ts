// Global type declarations

declare global {
  interface RegExpConstructor {
    /**
     * Escapes special characters in a string for use in a RegExp pattern.
     * See: https://github.com/tc39/proposal-regex-escaping
     * https://github.com/microsoft/TypeScript/issues/61321
     */
    escape(str: string): string;
  }

  interface Element {
    /**
     * Parses HTML, sanitizes it (removing scripts/event handlers), and sets as element content.
     * See: https://developer.mozilla.org/en-US/docs/Web/API/Element/setHTML
     */
    setHTML(input: string): void;
  }
}

export {};
