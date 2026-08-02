import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: { environment: 'jsdom', setupFiles: './src/test-setup.ts' },
  server: { proxy: { '/api': 'http://127.0.0.1:8080', '/metrics': 'http://127.0.0.1:8080' } },
})
