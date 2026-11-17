<template>
  <section class="page" data-module="data_mart">
    <header class="page-head">
      <div><h2>数据集市</h2><p class="page-desc">维护集市表，按事实键去重生成度量明细包，并用集市度量核对包验证消费结果。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记集市表</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>

    <div class="analysis-summary" style="margin-bottom:12px">
      <article class="analysis-card" data-testid="mart-kpi">
        <span style="font-size:12px;color:var(--muted)">明细包行数</span>
        <strong style="font-size:22px;display:block">{{ detail.row_count ?? 0 }}</strong>
        <span style="font-size:12px;color:var(--muted)">事实行数 {{ detail.fact_row_count ?? 0 }}</span>
      </article>
      <article class="analysis-card" data-testid="mart-kpi-measure">
        <span style="font-size:12px;color:var(--muted)">去重后度量合计</span>
        <strong style="font-size:22px;display:block">{{ formatAmount(detail.measure_total) }}</strong>
        <span style="font-size:12px;color:var(--muted)">避免重复累计 {{ formatAmount(detail.duplicate_measure_total) }}</span>
      </article>
      <article class="analysis-card" data-testid="mart-kpi-consistent">
        <span style="font-size:12px;color:var(--muted)">与事实表版本核对</span>
        <strong style="font-size:22px;display:block">{{ detail.consistent ? '一致' : '待核对' }}</strong>
        <span style="font-size:12px;color:var(--muted)">{{ detail.version ?? '—' }} · {{ modeLabel(detail.mode) }}</span>
      </article>
    </div>

    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <section class="mart-section" data-testid="measure-detail">
      <div class="section-head">
        <div>
          <h3>集市度量明细包</h3>
          <p class="page-desc">以集市事实数据与关联维度为输入，读取既有数据后按事实键去重并整理度量明细；度量只计一次，维度成员收进维度桥接说明。</p>
        </div>
        <div class="section-actions">
          <label class="inline-field">集市表
            <select v-model="detailTable" data-testid="detail-table" @change="onTableChange">
              <option v-for="v in versions" :key="v.table_no" :value="v.table_no">{{ v.table_no }}</option>
            </select>
          </label>
          <label class="inline-field">版本
            <select v-model="detailVersion" data-testid="detail-version" @change="loadDetail(1)">
              <option v-for="v in currentVersions" :key="v" :value="v">{{ v }}</option>
            </select>
          </label>
          <label class="inline-field">明细口径
            <select v-model="detailMode" data-testid="detail-mode" @change="loadDetail(1)">
              <option value="collapse">维度成员收进一条</option>
              <option value="allocate">按维度组合分摊度量</option>
            </select>
          </label>
          <button class="btn primary" type="button" data-testid="build-detail" @click="loadDetail(1)">生成明细包</button>
          <button class="btn" type="button" data-testid="export-measure-detail" @click="exportDetail">下载明细包</button>
        </div>
      </div>
      <p v-if="detailError" class="error-text">{{ detailError }}</p>
      <p class="bridge-note" data-testid="detail-bridge-note">{{ detail.bridge_note || '尚未生成度量明细包' }}</p>
      <table class="data-table" v-if="detailItems.length">
        <thead><tr><th>事实键</th><th>度量字段</th><th>度量值</th><th>维度成员</th><th>维度桥接说明</th><th>来源行数</th><th>重复累计</th></tr></thead>
        <tbody>
          <tr v-for="item in detailItems" :key="item.id" data-testid="detail-row">
            <td>{{ item.fact_key }}</td>
            <td>{{ item.measure_field }}</td>
            <td>{{ formatAmount(item.measure) }}</td>
            <td>{{ item.dimension_members.join('、') || '—' }}</td>
            <td>{{ item['维度桥接说明'] }}</td>
            <td>{{ item.source_rows }}</td>
            <td>{{ formatAmount(item.duplicate_measure) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state" style="padding:16px">当前版本没有可交付的度量明细行</p>
      <div class="page-foot" v-if="detail.total">
        <span>共 {{ detail.total }} 条明细 · 第 {{ detail.page }} / {{ detailPages }} 页</span>
        <span style="display:flex;gap:6px">
          <button class="btn" type="button" data-testid="detail-prev" :disabled="detail.page <= 1" @click="loadDetail(detail.page - 1)">上一页</button>
          <button class="btn" type="button" data-testid="detail-next" :disabled="detail.page >= detailPages" @click="loadDetail(detail.page + 1)">下一页</button>
        </span>
      </div>
    </section>

    <section class="mart-section" data-testid="reconciliation-packs">
      <div class="section-head">
        <div>
          <h3>集市度量核对包</h3>
          <p class="page-desc">核对接口编排核对包验证消费结果；支持导入校验、导出下载与上传失败回执，重复提交只写入一份有效结果。</p>
        </div>
        <div class="section-actions">
          <input v-model="packKeyword" data-testid="pack-keyword" placeholder="按包编号或名称检索" />
          <button class="btn" type="button" data-testid="pack-query" @click="loadPacks">查询</button>
          <button class="btn" type="button" data-testid="pack-reset" @click="resetPacks">重置</button>
          <button class="btn" type="button" data-testid="pack-export" @click="exportPacks">导出核对包</button>
          <button class="btn primary" type="button" data-testid="pack-reconcile" @click="reconcileLatest">核对最新包</button>
          <button class="btn" type="button" data-testid="pack-upload" @click="openImport">导入核对包</button>
        </div>
      </div>
      <p v-if="packError" class="error-text">{{ packError }}</p>
      <table class="data-table" v-if="packs.length">
        <thead><tr><th>核对包编号</th><th>名称</th><th>表编号</th><th>版本</th><th>明细口径</th><th>期望行数</th><th>期望度量合计</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="pack in packs" :key="pack.id" :data-testid="`pack-row-${pack.id}`">
            <td>{{ pack.pack_no }}</td>
            <td>{{ pack.name }}</td>
            <td>{{ pack.table_no }}</td>
            <td>{{ pack.version }}</td>
            <td>{{ modeLabel(pack.mode) }}</td>
            <td>{{ pack.expected_rows }}</td>
            <td>{{ formatAmount(pack.expected_measure) }}</td>
            <td :data-status="pack.status">{{ pack.status }}</td>
            <td class="row-actions">
              <button class="btn ghost" type="button" :data-testid="`pack-detail-${pack.id}`" @click="openPack(pack.id)">详情</button>
              <button class="btn ghost" type="button" :data-testid="`pack-check-${pack.id}`" @click="reconcilePack(pack.id)">核对</button>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-state" style="padding:16px">没有匹配的集市度量核对包</p>

      <div v-if="packDetail" class="pack-detail" data-testid="pack-detail">
        <h4>{{ packDetail.pack_no }} · {{ packDetail.name }}</h4>
        <dl class="pack-grid">
          <div><dt>集市表版本</dt><dd>{{ packDetail.table_no }} / {{ packDetail.version }} · {{ modeLabel(packDetail.mode) }}</dd></div>
          <div><dt>期望行数 / 实际行数</dt><dd>{{ packDetail.expected_rows }} / {{ packDetail.row_count }}</dd></div>
          <div><dt>期望度量 / 实际度量</dt><dd>{{ formatAmount(packDetail.expected_measure) }} / {{ formatAmount(packDetail.measure_total) }}</dd></div>
          <div><dt>事实行数 / 重复累计</dt><dd>{{ packDetail.fact_row_count }} / {{ formatAmount(packDetail.duplicate_measure_total) }}</dd></div>
          <div><dt>核对结论</dt><dd :data-status="packDetail.consistent ? '一致' : '存在冲突'">{{ packDetail.consistent ? '一致' : '存在冲突' }}</dd></div>
          <div><dt>维度桥接说明</dt><dd>{{ packDetail.bridge_note }}</dd></div>
        </dl>
        <div v-if="packDetail.conflicts?.length" class="conflict-list" data-testid="pack-conflicts">
          <p class="conflict-title">核对冲突（{{ packDetail.conflicts.length }} 项）</p>
          <ul>
            <li v-for="(conflict, i) in packDetail.conflicts" :key="i">
              <strong>{{ conflict.scope }}·{{ conflict.code }}</strong>：{{ conflict.message }}
            </li>
          </ul>
        </div>
        <p v-else class="ok-text">核对包与去重后的度量明细包一致</p>
      </div>

      <div class="receipt-block" data-testid="receipt-block">
        <h4>上传回执</h4>
        <ul v-if="receipts.length" class="receipt-list">
          <li v-for="receipt in receipts" :key="receipt.receipt_no" :data-receipt-status="receipt.status">
            <span>{{ receipt.receipt_no }}</span>
            <span>{{ receipt.pack_no }}</span>
            <span>{{ receipt.action }}</span>
            <strong>{{ receipt.status }}</strong>
            <span>{{ receipt.reasons.join('；') || '校验通过' }}</span>
            <span>{{ receipt.at }}</span>
          </li>
        </ul>
        <p v-else class="empty-state">暂无导入或上传回执</p>
      </div>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记集市表</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <div v-if="showImport" class="modal-backdrop" data-testid="import-modal">
      <form class="modal" @submit.prevent="submitImport"><h3>导入集市度量核对包</h3>
        <label class="create-field"><span>核对包编号</span><input v-model="importValues.pack_no" data-testid="import-pack-no" placeholder="PACK-0001" required /></label>
        <label class="create-field"><span>核对包名称</span><input v-model="importValues.name" data-testid="import-pack-name" placeholder="订单主题集市度量核对包" /></label>
        <label class="create-field"><span>表编号</span><input v-model="importValues.table_no" data-testid="import-table-no" placeholder="DATA-0003" required /></label>
        <label class="create-field"><span>版本</span><input v-model="importValues.version" data-testid="import-version" placeholder="V3" required /></label>
        <label class="create-field"><span>明细口径</span>
          <select v-model="importValues.mode" data-testid="import-mode">
            <option value="collapse">维度成员收进一条</option>
            <option value="allocate">按维度组合分摊度量</option>
          </select>
        </label>
        <label class="create-field"><span>期望行数</span><input v-model="importValues.expected_rows" data-testid="import-expected-rows" placeholder="3" /></label>
        <label class="create-field"><span>期望度量合计</span><input v-model="importValues.expected_measure" data-testid="import-expected-measure" placeholder="2500" /></label>
        <p v-if="importError" class="error-text">{{ importError }}</p>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showImport=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-import">上传</button>
        </div>
      </form>
    </div>

    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span><span>{{ packs.length }} 个核对包</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/data_mart'
const columns = ["表编号", "表名称", "主题域", "事实粒度", "关联维度", "度量字段", "刷新周期", "表状态"]
const actions = ["提交建模", "发布集市", "下线集市"]
const statuses = ["草稿", "已建模", "已发布", "已下线"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','已建模':'#fef3c7','已发布':'#d1fae5','已下线':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)

const detail = reactive<Record<string, any>>({ items: [], total: 0, page: 1, size: 20 })
const detailTable = ref('DATA-0003')
const detailVersion = ref('V3')
const detailMode = ref('collapse')
const versions = ref<{table_no: string; current: string; versions: string[]}[]>([])
const detailError = ref('')
const detailItems = computed<any[]>(() => detail.items || [])
const detailPages = computed(() => Math.max(1, Math.ceil((detail.total || 0) / (detail.size || 20))))

const packs = ref<any[]>([])
const packKeyword = ref('')
const packError = ref('')
const packDetail = ref<any>(null)
const receipts = ref<any[]>([])
const showImport = ref(false)
const importValues = ref<Record<string, string>>({})
const importError = ref('')

const currentVersions = computed(() => {
  const found = versions.value.find(v => v.table_no === detailTable.value)
  return found ? found.versions : [detailVersion.value]
})

function formatAmount(value: number | undefined) { return Number(value ?? 0).toFixed(2) }
function modeLabel(mode: string | undefined) { return mode === 'allocate' ? '按维度组合分摊度量' : '维度成员收进一条' }
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }

async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

async function loadVersions() {
  try {
    const res = await request(`${ENDPOINT}/versions`)
    const data = await res.json()
    versions.value = data.items || []
    const found = versions.value.find(v => v.table_no === detailTable.value)
    if (found) detailVersion.value = found.current
  } catch(e) { detailError.value = '版本信息加载失败' }
}
function onTableChange() {
  const found = versions.value.find(v => v.table_no === detailTable.value)
  if (found) detailVersion.value = found.current
  loadDetail(1)
}
async function loadDetail(page = 1) {
  detailError.value = ''
  const params = new URLSearchParams({
    table_no: detailTable.value,
    mode: detailMode.value,
    page: String(page),
    size: String(detail.size || 20),
  })
  if (detailVersion.value) params.set('version', detailVersion.value)
  try {
    const res = await request(`${ENDPOINT}/measure-detail?${params.toString()}`)
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '度量明细包生成失败')
    detail.items = data.items || []
    detail.total = data.total ?? 0
    detail.page = data.page ?? page
    detail.size = data.size ?? 20
    detail.row_count = data.row_count
    detail.measure_total = data.measure_total
    detail.fact_row_count = data.fact_row_count
    detail.duplicate_measure_total = data.duplicate_measure_total
    detail.consistent = data.consistent
    detail.version = data.version
    detail.mode = data.mode
    detail.bridge_note = data.bridge_note
  } catch(e: any) { detailError.value = e.message; detail.items = []; detail.total = 0 }
}
function exportDetail() {
  const params = new URLSearchParams({ table_no: detailTable.value, mode: detailMode.value })
  if (detailVersion.value) params.set('version', detailVersion.value)
  window.open(`${ENDPOINT}/measure-detail/export?${params.toString()}`, '_blank')
}

async function loadPacks() {
  packError.value = ''
  const params = new URLSearchParams({ page: '1', size: '50' })
  if (packKeyword.value.trim()) params.set('keyword', packKeyword.value.trim())
  try {
    const res = await request(`${ENDPOINT}/reconciliation-packs?${params.toString()}`)
    const data = await res.json()
    packs.value = data.items || []
  } catch(e) { packError.value = '核对包列表加载失败' }
}
function resetPacks() { packKeyword.value = ''; packDetail.value = null; loadPacks() }
async function openPack(packId: number) {
  packError.value = ''
  try {
    const res = await request(`${ENDPOINT}/reconciliation-packs/${packId}`)
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '核对包详情加载失败')
    packDetail.value = data
  } catch(e: any) { packError.value = e.message }
}
async function reconcilePack(packId: number) {
  try {
    const res = await request(`${ENDPOINT}/reconciliation-packs/${packId}/reconcile`, { method: 'POST' })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message || '核对失败')
    await loadPacks()
    await openPack(packId)
    await loadReceipts()
  } catch(e: any) { packError.value = e.message }
}
async function reconcileLatest() {
  try {
    const res = await request(`${ENDPOINT}/reconciliation`, { method: 'POST' })
    const data = await res.json()
    if (!data.ok) throw new Error(data.message || '核对失败')
    await loadPacks()
    if (data.entry?.pack_id) await openPack(data.entry.pack_id)
  } catch(e: any) { packError.value = e.message }
}
function exportPacks() { window.open(`${ENDPOINT}/reconciliation-packs/export`, '_blank') }
function openImport() { importValues.value = { pack_no: '', name: '', table_no: 'DATA-0003', version: 'V3', mode: 'collapse', expected_rows: '', expected_measure: '' }; importError.value = ''; showImport.value = true }
async function submitImport() {
  importError.value = ''
  const values: Record<string, any> = { ...importValues.value }
  if (values.expected_rows !== '') values.expected_rows = Number(values.expected_rows)
  if (values.expected_measure !== '') values.expected_measure = Number(values.expected_measure)
  try {
    const res = await request(`${ENDPOINT}/reconciliation-packs/import`, { method: 'POST', body: JSON.stringify({ values }) })
    const data = await res.json()
    await loadReceipts()
    if (!data.ok) { importError.value = data.message; return }
    showImport.value = false
    await loadPacks()
  } catch(e: any) { importError.value = e.message }
}
async function loadReceipts() {
  try { const res = await request(`${ENDPOINT}/reconciliation-packs/receipts`); receipts.value = (await res.json()).items || [] } catch(e) {}
}

onMounted(async () => { await reload(); await loadVersions(); await loadDetail(1); await loadPacks(); await loadReceipts() })
</script>
<style scoped>
.mart-section { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-top: 14px; }
.section-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
.section-head h3 { margin: 0; font-size: 15px; }
.section-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.section-actions input { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.inline-field { display: flex; align-items: center; gap: 4px; font-size: 12px; color: var(--muted); }
.inline-field select { padding: 5px 8px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.bridge-note { background: #f1f5f9; border-left: 3px solid var(--brand); padding: 8px 10px; font-size: 12px; color: #334155; margin: 10px 0; }
.pack-detail { margin-top: 12px; border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: #fafbfc; }
.pack-detail h4 { margin: 0 0 8px; font-size: 14px; }
.pack-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px; margin: 0; }
.pack-grid dt { font-size: 12px; color: var(--muted); }
.pack-grid dd { font-size: 13px; margin: 2px 0 0; }
.conflict-list { margin-top: 10px; background: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; padding: 8px 12px; }
.conflict-title { margin: 0 0 6px; color: #b42318; font-size: 13px; }
.conflict-list ul { margin: 0; padding-left: 18px; font-size: 12px; color: #7f1d1d; }
.ok-text { color: #15803d; font-size: 13px; margin: 10px 0 0; }
.receipt-block { margin-top: 14px; }
.receipt-block h4 { font-size: 14px; margin: 0 0 8px; }
.receipt-list { list-style: none; margin: 0; padding: 0; font-size: 12px; }
.receipt-list li { display: flex; gap: 10px; flex-wrap: wrap; padding: 6px 0; border-bottom: 1px dashed var(--border); }
.receipt-list li strong { color: #b42318; }
.receipt-list li[data-receipt-status="上传成功"] strong { color: #15803d; }
</style>
