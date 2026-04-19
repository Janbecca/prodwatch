<!-- 作用：前端页面：帖子列表视图。 -->

<template>
  <div class="pw-page">
    <el-empty v-if="!hasProjects" description="暂无启用项目" />
    <template v-else>
      <PostsFilters />
      <PostsOverview />
      <PostsListTable />
    </template>
  </div>

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
