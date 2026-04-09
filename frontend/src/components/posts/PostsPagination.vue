<!-- 作用：前端组件：帖子模块组件（PostsPagination）。 -->

<template>
  <el-pagination
    :current-page="store.page"
    :page-size="store.pageSize"
    :page-sizes="[10, 20, 50, 100]"
    layout="total, sizes, prev, pager, next, jumper"
    :total="store.total"
    background
    :disabled="!store.applied || store.listLoading"
    @update:current-page="store.setPage"
    @update:page-size="store.setPageSize"
  />
</template>

<script setup>
import { usePostsStore } from '../../stores/posts'

const store = usePostsStore()
</script>
