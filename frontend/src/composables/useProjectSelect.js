// 作用：前端组合式函数：useProjectSelect。

import { computed } from 'vue'
import { useProjectsStore } from '../stores/projects'

export function useProjectSelect() {
  const store = useProjectsStore()

  const model = computed({
    get: () => store.activeProjectId,
    set: (v) => store.setActiveProject(v),
  })

  const options = computed(() => store.projects)
  const disabled = computed(() => store.loading || options.value.length === 0)

  return { store, model, options, disabled }
}

