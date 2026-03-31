<template>
  <el-space direction="vertical" :size="12" fill>
    <el-empty v-if="!hasProjects" description="No enabled project" />
    <template v-else>
      <PostsFilters />
      <PostsOverview />
      <PostsListTable />
    </template>
  </el-space>

  <PostDetailDrawer />
</template>

<script setup>
import { computed } from 'vue'
import { useProjectsStore } from '../stores/projects'

import PostsFilters from '../components/posts/PostsFilters.vue'
import PostsOverview from '../components/posts/PostsOverview.vue'
import PostsListTable from '../components/posts/PostsListTable.vue'
import PostDetailDrawer from '../components/posts/PostDetailDrawer.vue'

const projectsStore = useProjectsStore()
const hasProjects = computed(() => projectsStore.projects.length > 0)
</script>
