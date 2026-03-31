<template>
  <PageSection title="项目配置表单">
    <el-form ref="formRef" :model="model" :rules="rules" label-width="110px" status-icon>
      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="项目ID">
            <el-input :model-value="model.id ?? '—'" disabled />
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="项目名称" prop="name">
            <el-input v-model="model.name" :disabled="readOnly" />
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="产品品类">
            <el-input v-model="model.product_category" :disabled="readOnly" />
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="我方品牌">
            <el-select v-model="model.our_brand_id" :disabled="readOnly" style="width: 100%">
              <el-option v-for="b in brandOptions" :key="b.id" :label="b.name" :value="b.id" />
            </el-select>
          </el-form-item>
        </el-col>
      </el-row>

      <el-form-item label="项目描述">
        <el-input v-model="model.description" type="textarea" :rows="3" :disabled="readOnly" />
      </el-form-item>

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="是否启用">
            <el-switch v-model="model.is_active" :disabled="true" />
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="项目状态">
            <el-select v-model="model.status" :disabled="readOnly" style="width: 100%">
              <el-option label="draft" value="draft" />
              <el-option label="inactive" value="inactive" />
              <el-option label="active" value="active" />
              <el-option label="archived" value="archived" />
            </el-select>
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="刷新方式">
            <el-select v-model="model.refresh_mode" :disabled="readOnly" style="width: 100%">
              <el-option label="manual" value="manual" />
              <el-option label="daily" value="daily" />
            </el-select>
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="自动刷新表达式">
            <el-input v-model="model.refresh_cron" :disabled="readOnly" placeholder="cron 表达式" />
          </el-form-item>
        </el-col>
      </el-row>

      <el-divider />

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="品牌" required>
            <el-select
              v-model="model.brand_ids"
              multiple
              collapse-tags
              collapse-tags-tooltip
              :disabled="readOnly"
              style="width: 100%"
            >
              <el-option v-for="b in brandOptions" :key="b.id" :label="b.name" :value="b.id" />
            </el-select>
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-form-item label="平台" required>
            <el-select
              v-model="model.platform_ids"
              multiple
              collapse-tags
              collapse-tags-tooltip
              :disabled="readOnly"
              style="width: 100%"
            >
              <el-option v-for="p in platformOptions" :key="p.id" :label="p.name" :value="p.id" />
            </el-select>
          </el-form-item>
        </el-col>
      </el-row>

      <el-form-item label="关键词" required>
        <el-table :data="model.keywords" stripe style="width: 100%">
          <el-table-column label="keyword" min-width="200">
            <template #default="{ row }">
              <el-input v-model="row.keyword" :disabled="readOnly" />
            </template>
          </el-table-column>
          <el-table-column label="keyword_type" width="160">
            <template #default="{ row }">
              <el-input v-model="row.keyword_type" :disabled="readOnly" placeholder="feature/issue/..." />
            </template>
          </el-table-column>
          <el-table-column label="weight" width="120">
            <template #default="{ row }">
              <el-input-number v-model="row.weight" :disabled="readOnly" :min="0" :max="999" />
            </template>
          </el-table-column>
          <el-table-column label="is_enabled" width="140">
            <template #default="{ row }">
              <el-switch v-model="row.is_enabled" :disabled="readOnly" :active-value="1" :inactive-value="0" />
            </template>
          </el-table-column>
          <el-table-column v-if="!readOnly" label="操作" width="120">
            <template #default="{ $index }">
              <el-button size="small" type="danger" plain @click="removeKeyword($index)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>

        <div v-if="!readOnly" class="kw-actions">
          <el-button plain @click="addKeyword()">新增关键词</el-button>
        </div>
      </el-form-item>
    </el-form>
  </PageSection>
</template>

<script setup>
import { computed, ref } from 'vue'
import PageSection from '../common/PageSection.vue'

const props = defineProps({
  mode: { type: String, default: 'view' }, // create/edit
  model: { type: Object, required: true },
  brandOptions: { type: Array, default: () => [] },
  platformOptions: { type: Array, default: () => [] },
})

const formRef = ref()

const readOnly = computed(() => false)

const rules = {
  name: [{ required: true, message: '项目名称必填', trigger: 'blur' }],
}

function addKeyword() {
  props.model.keywords.push({ keyword: '', keyword_type: '', weight: 0, is_enabled: 1 })
}

function removeKeyword(idx) {
  props.model.keywords.splice(idx, 1)
}

async function validate() {
  await formRef.value.validate()
  // additional validations:
  if (!props.model.brand_ids || props.model.brand_ids.length < 1) {
    throw new Error('品牌至少 1 个')
  }
  if (!props.model.platform_ids || props.model.platform_ids.length < 1) {
    throw new Error('平台至少 1 个')
  }
  if (!props.model.keywords || props.model.keywords.length < 1) {
    throw new Error('关键词至少 1 组')
  }
  if (props.model.keywords.some((k) => !k.keyword || String(k.keyword).trim() === '')) {
    throw new Error('关键词 keyword 不能为空')
  }
}

defineExpose({ validate })
</script>

<style scoped>
.kw-actions {
  margin-top: 10px;
  display: flex;
  justify-content: flex-end;
}
</style>
