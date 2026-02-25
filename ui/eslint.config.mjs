import { FlatCompat } from '@eslint/eslintrc'

const compat = new FlatCompat({
  // import.meta.dirname is available after Node.js v20.11.0
  baseDirectory: import.meta.dirname,
})

const eslintConfig = [
  ...compat.config({
    extends: ['next/core-web-vitals', 'next/typescript'],
  }),
  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    rules: {
      'indent': ['error', 2],
      '@next/next/no-img-element': 'off',
      'import/order': [
        'warn',
        {
          groups: [
            'builtin',
            'external',
            'internal',
            'parent',
            'sibling',
            'index',
            'object',
            'type'
          ],
          'newlines-between': 'always',
          pathGroups: [
            {
              // Style imports last
              pattern: '**/*.{css,scss,sass,less}',
              group: 'type',
              position: 'after'
            }
          ],
          alphabetize: {
            order: 'asc',
            caseInsensitive: true
          },
          warnOnUnassignedImports: true
        }
      ],
    }
  }
]

export default eslintConfig
