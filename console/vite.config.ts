import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  resolve: {
    dedupe: ['react', 'react-dom'],
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test-setup.ts',
    // react-query ships ESM-only; inlining it routes its `react` import through
    // Vite's resolver (where the dedupe rule above pins one React instance)
    // instead of letting Node resolve a second copy relative to the package.
    server: { deps: { inline: [/@tanstack\/react-query/] } },
  },
  server: { proxy: { '/api': 'http://127.0.0.1:8080', '/metrics': 'http://127.0.0.1:8080' } },
})
