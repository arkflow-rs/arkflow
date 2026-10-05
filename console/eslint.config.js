import js from '@eslint/js'
import tseslint from 'typescript-eslint'
import reactHooks from 'eslint-plugin-react-hooks'
import prettier from 'eslint-config-prettier'

export default tseslint.config(
  { ignores: ['dist', 'coverage', 'node_modules'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['src/**/*.{ts,tsx}'],
    plugins: {
      'react-hooks': reactHooks,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      // Test doubles and mock fetch handlers routinely ignore parameters
      // that the real signatures require.
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
    },
  },
  {
    files: ['src/**/*.tsx'],
    languageOptions: {
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
    },
  },
  // Prettier owns formatting; eslint must not fight it.
  prettier,
  {
    // The DAG editor and its graph helpers consume free-form plan JSON
    // whose shape is validated at runtime by the server contract tests;
    // typing these payloads is tracked separately from lint hygiene.
    files: ['src/features/job-dag.ts', 'src/features/job-editor.tsx'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
    },
  },
)
