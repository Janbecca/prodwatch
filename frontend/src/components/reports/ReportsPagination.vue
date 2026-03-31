<template>
  <el-pagination
    :current-page="store.page"
    :page-size="store.pageSize"
    :page-sizes="[10, 20, 50, 100]"
    layout="total, sizes, prev, pager, next, jumper"
    :total="store.total"
    background
    :disabled="!store.applied || store.loading"
    @update:current-page="store.setPage"
    @update:page-size="store.setPageSize"
  />
</template>

<script setup>
import { useReportsStore } from '../../stores/reports'

const store = useReportsStore()
</script>

