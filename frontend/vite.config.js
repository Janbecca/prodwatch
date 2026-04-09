// 作用：前端：Vite 构建与开发服务器配置。

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
        // Manual refresh runs the whole pipeline synchronously in dev/demo and may exceed 10s.
        // Keep a generous timeout to avoid the browser seeing net::ERR_EMPTY_RESPONSE.
        timeout: 300_000,
        proxyTimeout: 300_000,
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
