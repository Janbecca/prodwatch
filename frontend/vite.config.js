import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
        timeout: 10_000,
        proxyTimeout: 10_000,
        configure(proxy) {
          proxy.on('error', (err, _req, res) => {
            if (!res.writeHead) return
            res.writeHead(502, { 'Content-Type': 'application/json' })
            res.end(JSON.stringify({ error: 'proxy_error', message: String(err?.message || err) }))
          })
        },
      },
    },
  },
})
