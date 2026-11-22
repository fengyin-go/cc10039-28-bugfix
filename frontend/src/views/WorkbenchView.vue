<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in availableActions(row)"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="hasDetail(row)"
              class="link"
              type="button"
              :data-testid="`detail-${row.id}`"
              @click="openDetail(row)"
            >
              查看详情
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal">
      <section class="modal detail-modal">
        <header class="detail-head">
          <h3>{{ meta.entity }}详情 · #{{ detail.id }}</h3>
          <button class="btn ghost" type="button" data-testid="close-detail" @click="closeDetail">关闭</button>
        </header>
        <p v-if="detailMessage" class="detail-note" :class="{ 'error-text': detailBlocked }" data-testid="detail-message">
          {{ detailMessage }}
        </p>
        <dl class="detail-grid">
          <template v-for="field in meta.fields" :key="field">
            <dt>{{ field }}</dt>
            <dd>{{ detail[field] ?? '—' }}</dd>
          </template>
          <dt>当前状态</dt>
          <dd>{{ detail.status }}</dd>
        </dl>

        <template v-if="detailPartitions.length">
          <h4 class="detail-title">剩余分区（{{ detailPartitions.length }}）</h4>
          <table class="data-table compact">
            <thead>
              <tr>
                <th>分区</th>
                <th>读取位置</th>
                <th>来源</th>
                <th>已补条数</th>
                <th>可读</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="item in detailPartitions"
                :key="String(item.partition)"
                :data-testid="`partition-${item.partition}`"
              >
                <td>{{ item.partition }}</td>
                <td class="mono">{{ item.read_location ?? '—' }}</td>
                <td>{{ item.source }}</td>
                <td>{{ item.backfilled_count ?? item.count ?? 0 }}</td>
                <td>{{ item.readable === false ? '否' : '是' }}</td>
              </tr>
            </tbody>
          </table>
        </template>

        <template v-if="detailCompleted.length">
          <h4 class="detail-title">已完成分区（{{ detailCompleted.length }}）</h4>
          <p class="detail-note">
            <span v-for="item in detailCompleted" :key="String(item.partition)" class="legend-item">
              {{ item.partition }}：{{ item.backfilled_count ?? item.count ?? 0 }} 条
            </span>
          </p>
        </template>

        <template v-if="detailReceipts.length">
          <h4 class="detail-title">保留回执（{{ detailReceipts.length }}）</h4>
          <table class="data-table compact">
            <thead>
              <tr>
                <th>分区</th>
                <th>清理动作</th>
                <th>读取位置</th>
                <th>可读</th>
                <th>已补条数</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in detailReceipts" :key="index">
                <td>{{ item.partition }}</td>
                <td>{{ item.action }}</td>
                <td class="mono">{{ item.read_location ?? '—' }}</td>
                <td>{{ item.readable === false ? '否' : '是' }}</td>
                <td>{{ item.backfilled_count ?? 0 }}</td>
              </tr>
            </tbody>
          </table>
        </template>

        <template v-if="detailConflicts.length">
          <h4 class="detail-title">清理冲突（{{ detailConflicts.length }}）</h4>
          <ul class="detail-list" data-testid="detail-conflicts">
            <li v-for="(item, index) in detailConflicts" :key="index">
              {{ item.partition }}：{{ item.reason }}
            </li>
          </ul>
        </template>

        <div class="page-actions">
          <button
            v-if="canResume"
            class="btn primary"
            type="button"
            data-testid="detail-resume"
            @click="resumeFromDetail"
          >
            续做回填
          </button>
        </div>
      </section>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  createEntry,
  downloadEntries,
  getEntryDetail,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

/** 只有这些模块有额外的详情视图（可续做分区清单 / 读取位置和回执）。 */
const DETAIL_MODULES = new Set(['backfill_job', 'retention_mgr'])
/** 只在这些状态下出现的动作，避免把不可执行的动作渲染成可点按钮。 */
const STATE_ACTIONS: Record<string, Record<string, string[]>> = {
  backfill_job: { 续做回填: ['回填失败'] },
  retention_mgr: { 执行清理: ['已生效'] },
}

type PartitionRow = Record<string, string | number | boolean | null | undefined>

const props = defineProps<{ moduleKey: string }>()
// 路由在同一组件里切换模块时 props 会变，meta 必须跟着算，不能只取一次。
const meta = computed(() => moduleMeta(props.moduleKey))
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = computed(() => meta.value.fields.slice(0, 3))
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const detail = ref<Record<string, unknown> | null>(null)
const detailMessage = ref('')
const detailBlocked = ref(false)
const stats = computed(() => [
  { label: meta.value.metrics[0], value: total.value },
  {
    label: meta.value.metrics[1],
    value: rows.value.filter((row) => row.status === meta.value.statuses[1]).length,
  },
  {
    label: meta.value.metrics[2],
    value: rows.value.filter(
      (row) => row.status === meta.value.statuses[meta.value.statuses.length - 1],
    ).length,
  },
])
const statusSummary = computed(() =>
  meta.value.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

const detailPartitions = computed<PartitionRow[]>(() => {
  const items = detail.value?.剩余分区
  return Array.isArray(items) ? (items as PartitionRow[]) : []
})
const detailCompleted = computed<PartitionRow[]>(() => {
  const items = detail.value?.已完成分区
  return Array.isArray(items) ? (items as PartitionRow[]) : []
})
const detailReceipts = computed<PartitionRow[]>(() => {
  const items = detail.value?.保留回执
  return Array.isArray(items) ? (items as PartitionRow[]) : []
})
const detailConflicts = computed<PartitionRow[]>(() => {
  const items = detail.value?.冲突
  return Array.isArray(items) ? (items as PartitionRow[]) : []
})
const canResume = computed(() => Boolean(detail.value?.可续做))

function availableActions(row: EntryRow): string[] {
  const overrides = STATE_ACTIONS[meta.value.key] ?? {}
  return meta.value.actions.filter((action) => {
    const allowed = overrides[action]
    return !allowed || allowed.includes(String(row.status))
  })
}

function hasDetail(row: EntryRow): boolean {
  return DETAIL_MODULES.has(meta.value.key) && Boolean(row.id)
}

function resetDetail() {
  detail.value = null
  detailMessage.value = ''
  detailBlocked.value = false
}

async function openDetail(row: EntryRow) {
  errorMessage.value = ''
  try {
    const payload = await getEntryDetail(meta.value.key, Number(row.id))
    detail.value = payload
    detailBlocked.value = false
    detailMessage.value = detailSummaryText(payload)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '详情读取失败'
  }
}

function detailSummaryText(payload: Record<string, unknown>): string {
  if (meta.value.key === 'backfill_job') {
    return String(payload.续做说明 ?? '')
  }
  if (meta.value.key === 'retention_mgr') {
    const summary = payload.summary as Record<string, number> | undefined
    if (!summary) return ''
    return `剩余分区 ${summary.remaining_count ?? summary.total} 个，可读 ${summary.readable} 个，`
      + `剩余待补 ${summary.remaining_count_sum ?? 0} 条，已完成 ${summary.completed_count ?? 0} 条，`
      + `保留回执 ${summary.receipt_count} 条`
  }
  return ''
}

function closeDetail() {
  resetDetail()
}

async function resumeFromDetail() {
  if (!detail.value) return
  await runAction('续做回填', { id: Number(detail.value.id) } as EntryRow)
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.value.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.value.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.value.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.value.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
    if (detail.value && Number(detail.value.id) === Number(row.id)) {
      await openDetail(row)
      detailMessage.value = result.message
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : '操作失败'
    if (detail.value && Number(detail.value.id) === Number(row.id)) {
      detailMessage.value = message
      detailBlocked.value = true
    } else {
      errorMessage.value = message
    }
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.value.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

watch(
  () => props.moduleKey,
  () => {
    resetDetail()
    showCreate.value = false
    filters.value = {}
    void reload()
  },
)

onMounted(reload)
</script>
