<!-- 作用：前端组件：项目配置模块组件（ProjectEditForm）。 -->

<template>
  <PageSection title="项目配置表单">
    <el-form ref="formRef" :model="model" :rules="rules" label-width="110px" status-icon>
      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="项目编号">
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
      </el-row>

      <el-form-item label="项目描述">
        <el-input v-model="model.description" type="textarea" :rows="3" :disabled="readOnly" />
      </el-form-item>

      <el-row :gutter="12">
        <el-col :span="12">
          <el-form-item label="刷新方式">
            <el-select v-model="model.refresh_mode" :disabled="readOnly" style="width: 100%">
              <el-option label="手动" value="manual" />
              <el-option label="每天" value="daily" />
            </el-select>
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
              filterable
              allow-create
              default-first-option
              reserve-keyword
            >
              <el-option v-for="b in localBrandOptions" :key="b.id" :label="b.name" :value="b.id">
                <div class="brand-opt">
                  <span class="brand-opt__name">{{ b.name }}</span>
                  <el-button
                    link
                    type="danger"
                    size="small"
                    class="brand-opt__del"
                    :disabled="!b.is_deletable || deletingBrandIds.has(b.id)"
                    @click.stop.prevent="onDeleteBrand(b)"
                  >
                    删除
                  </el-button>
                </div>
              </el-option>
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
          <el-table-column label="关键词" min-width="200">
            <template #default="{ row }">
              <el-input v-model="row.keyword" :disabled="readOnly" />
            </template>
          </el-table-column>
          <el-table-column label="权重" width="120">
            <template #default="{ row }">
              <el-input-number v-model="row.weight" :disabled="readOnly" :min="0" :max="999" />
            </template>
          </el-table-column>
          <el-table-column label="是否启用" width="140">
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
import { ElMessage, ElMessageBox } from 'element-plus'
import { computed, ref, watch } from 'vue'
import PageSection from '../common/PageSection.vue'
import { createBrand, deleteBrand } from '../../api/meta'

const props = defineProps({
  mode: { type: String, default: 'view' }, // create/edit
  model: { type: Object, required: true },
  brandOptions: { type: Array, default: () => [] },
  platformOptions: { type: Array, default: () => [] },
})

const emit = defineEmits(['refresh-meta'])

 const formRef = ref()

 const readOnly = computed(() => false)

 const localBrandOptions = ref(Array.isArray(props.brandOptions) ? [...props.brandOptions] : [])
const creatingBrandNames = new Set()
const deletingBrandIds = ref(new Set())

watch(
  () => props.brandOptions,
  (next) => {
    localBrandOptions.value = Array.isArray(next) ? [...next] : []
  },
  { immediate: true },
)

watch(
  () => (Array.isArray(props.model?.brand_ids) ? props.model.brand_ids.slice() : []),
  async (next) => {
    const pending = next.filter((x) => typeof x === 'string' && String(x).trim() !== '')
    for (const raw of pending) {
      const name = String(raw).trim()
      if (!name) continue
      if (creatingBrandNames.has(name)) continue
      creatingBrandNames.add(name)
      try {
        const res = await createBrand({ name })
        const brand = res?.brand
        const brandId = Number(brand?.id)
        if (!Number.isFinite(brandId) || brandId <= 0) throw new Error('创建品牌失败：返回的品牌编号无效')

        if (!localBrandOptions.value.some((b) => Number(b?.id) === brandId)) {
          localBrandOptions.value.push(brand)
        }

        const idx = props.model.brand_ids.findIndex((v) => v === raw)
        if (idx >= 0) props.model.brand_ids.splice(idx, 1)
        if (!props.model.brand_ids.some((v) => Number(v) === brandId)) {
          props.model.brand_ids.push(brandId)
        }

        emit('refresh-meta')
      } catch (e) {
        const msg = e?.message || String(e)
        ElMessage.error(msg)
        const idx = props.model.brand_ids.findIndex((v) => v === raw)
        if (idx >= 0) props.model.brand_ids.splice(idx, 1)
      } finally {
        creatingBrandNames.delete(name)
      }
    }
  },
  { immediate: true },
)

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
    throw new Error('关键词不能为空')
  }
}

async function onDeleteBrand(b) {
  const bid = Number(b?.id)
  if (!Number.isFinite(bid) || bid <= 0) return
  if (!b?.is_deletable) {
    ElMessage.warning('该品牌已关联数据，无法删除')
    return
  }

  try {
    await ElMessageBox.confirm(`确认删除品牌「${b?.name || bid}」吗？`, '删除确认', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }

  const set = deletingBrandIds.value
  set.add(bid)
  deletingBrandIds.value = new Set(set)
  try {
    await deleteBrand(bid)
    localBrandOptions.value = localBrandOptions.value.filter((x) => Number(x?.id) !== bid)
    if (Array.isArray(props.model?.brand_ids)) {
      props.model.brand_ids = props.model.brand_ids.filter((x) => Number(x) !== bid)
    }
    if (Number(props.model?.our_brand_id) === bid) {
      props.model.our_brand_id = null
    }
    emit('refresh-meta')
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(e?.message || String(e))
    emit('refresh-meta')
  } finally {
    const set2 = deletingBrandIds.value
    set2.delete(bid)
    deletingBrandIds.value = new Set(set2)
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

.brand-opt {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}
.brand-opt__name {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.brand-opt__del {
  flex: 0 0 auto;
}
</style>
