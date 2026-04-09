<!-- 作用：前端组件：通用模块组件（SafeMarkdown）。 -->

<template>
  <div class="md" v-html="html" />
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  markdown: { type: String, default: '' },
})

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function safeHref(url) {
  const u = String(url || '').trim()
  if (!u) return '#'
  if (u.startsWith('http://') || u.startsWith('https://')) return u
  if (u.startsWith('/')) return u
  return '#'
}

function renderInline(text) {
  let s = escapeHtml(text)
  // inline code
  s = s.replace(/`([^`]+)`/g, (_m, code) => `<code>${escapeHtml(code)}</code>`)
  // bold
  s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
  // italic (avoid matching inside bold by keeping it simple)
  s = s.replace(/\*([^*]+)\*/g, '<em>$1</em>')
  // links
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_m, label, url) => {
    const href = safeHref(url)
    const t = escapeHtml(label)
    return `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${t}</a>`
  })
  return s
}

function markdownToSafeHtml(md) {
  const src = String(md || '').replace(/\r\n/g, '\n')

  // Handle fenced code blocks first.
  const parts = src.split(/```/g)
  const out = []
  for (let i = 0; i < parts.length; i++) {
    const chunk = parts[i]
    if (i % 2 === 1) {
      // code block, drop optional language line
      const lines = chunk.split('\n')
      if (lines.length && /^[a-zA-Z0-9_-]+$/.test(lines[0].trim())) lines.shift()
      out.push(`<pre><code>${escapeHtml(lines.join('\n'))}</code></pre>`)
      continue
    }

    const lines = chunk.split('\n')
    let inList = false
    for (const line of lines) {
      const raw = String(line || '')
      const t = raw.trimEnd()
      if (t.trim() === '') {
        if (inList) {
          out.push('</ul>')
          inList = false
        }
        continue
      }

      const m = t.match(/^(#{1,6})\s+(.*)$/)
      if (m) {
        if (inList) {
          out.push('</ul>')
          inList = false
        }
        const level = m[1].length
        out.push(`<h${level}>${renderInline(m[2])}</h${level}>`)
        continue
      }

      const li = t.match(/^\-\s+(.*)$/)
      if (li) {
        if (!inList) {
          out.push('<ul>')
          inList = true
        }
        out.push(`<li>${renderInline(li[1])}</li>`)
        continue
      }

      if (inList) {
        out.push('</ul>')
        inList = false
      }
      out.push(`<p>${renderInline(t)}</p>`)
    }
    if (inList) out.push('</ul>')
  }
  return out.join('\n')
}

const html = computed(() => markdownToSafeHtml(props.markdown))
</script>

<style scoped>
.md :deep(pre) {
  padding: 10px 12px;
  border-radius: 8px;
  background: var(--el-fill-color-blank);
  border: 1px solid var(--el-border-color-lighter);
  overflow: auto;
}
.md :deep(code) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
}
.md :deep(h1),
.md :deep(h2),
.md :deep(h3) {
  margin: 12px 0 8px;
}
.md :deep(p) {
  margin: 6px 0;
}
</style>
