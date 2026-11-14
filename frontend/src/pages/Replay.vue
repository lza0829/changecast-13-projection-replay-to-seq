<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const streamId = ref(null)
const targetSeq = ref(3)
const batchSize = ref(2)
const holdSeconds = ref(0)
const forceFail = ref(false)
const replays = ref([])
const locks = ref([])
const detail = ref(null)
const diff = ref(null)
const busy = ref(false)
const err = ref('')
const info = ref('')

const selectedStream = computed(
  () => streams.value.find((s) => s.id === Number(streamId.value)) || null
)

async function refresh() {
  streams.value = await getJSON('/api/streams')
  replays.value = await getJSON('/api/replays?limit=50')
  locks.value = await getJSON('/api/stream-locks')
  if (streamId.value == null && streams.value.length) streamId.value = streams.value[0].id
}

async function loadDetail(id) {
  err.value = ''
  info.value = ''
  try {
    detail.value = await getJSON(`/api/replays/${id}`)
    diff.value = ['completed', 'promoted'].includes(detail.value.status)
      ? await getJSON(`/api/replays/${id}/diff`)
      : null
  } catch (e) {
    err.value = String(e.message || e)
  }
}

function explainError(e) {
  try {
    const parsed = JSON.parse(e.message?.replace(/^Error: /, '') || '{}')
    if (parsed?.detail?.error === 'stream_locked') {
      const h = parsed.detail.holder
      return `409 互斥：该流正被 ${h.operation}（${h.holder_ref}）占用，重放/开跑/提升同流互斥，已拒绝本次操作，主投影未受影响。`
    }
    if (parsed?.detail) return typeof parsed.detail === 'string' ? parsed.detail : JSON.stringify(parsed.detail)
  } catch {
    /* fall through */
  }
  return String(e.message || e)
}

async function startReplay() {
  err.value = ''
  info.value = ''
  busy.value = true
  try {
    const r = await postJSON('/api/replays', {
      stream_id: Number(streamId.value),
      target_seq: Number(targetSeq.value),
      batch_size: Number(batchSize.value),
      hold_seconds: Number(holdSeconds.value),
      force_fail: forceFail.value,
    })
    info.value = `重放 ${r.replay_no} 完成：目标 seq ${r.target_seq}，${r.batch_count} 个批次，快照 ${r.rows_total} 行，互证全部通过。`
    await refresh()
    await loadDetail(r.id)
  } catch (e) {
    err.value = explainError(e)
    await refresh()
  } finally {
    busy.value = false
  }
}

async function promote() {
  if (!detail.value) return
  err.value = ''
  info.value = ''
  if (!window.confirm(`确认把重放 ${detail.value.replay_no}（seq ${detail.value.target_seq}）提升为主投影？主水位将只经此接口变更。`)) return
  busy.value = true
  try {
    const r = await postJSON(`/api/replays/${detail.value.id}/promote`)
    info.value = `提升完成：新主水位 seq ${r.watermark_seq}，主投影 ${r.rows_total} 行（存活 ${r.rows_alive} / 已删 ${r.rows_deleted}），批次 ${r.batch_no}。`
    await refresh()
    await loadDetail(detail.value.id)
  } catch (e) {
    err.value = explainError(e)
    await refresh()
  } finally {
    busy.value = false
  }
}

function statusTag(status) {
  return { completed: 'ok', promoted: 'ok', failed: 'danger', running: 'warn' }[status] || 'warn'
}

onMounted(async () => {
  try {
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>

<template>
  <div class="page">
    <h1>按 seq 重放</h1>
    <p class="muted">
      从事件源重放到指定 seq 生成目标快照，隔离存放于 replay_* 表，不回拨、不改写主水位。
      重放与常规开跑同流互斥；提升为主投影走独立接口并复核截断点。这不是「把水位改小再跑增量」。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="info" class="tag ok">{{ info }}</p>

    <div class="panel">
      <h3 style="margin-top:0">发起目标 seq 重放</h3>
      <div style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end">
        <label>
          投影流
          <select v-model.number="streamId" style="display:block;min-width:16rem;margin-top:0.25rem">
            <option v-for="s in streams" :key="s.id" :value="s.id">
              {{ s.title }}（主水位 {{ s.watermark_seq }}）
            </option>
          </select>
        </label>
        <label>
          目标 seq
          <input v-model.number="targetSeq" type="number" min="0" style="display:block;width:8rem;margin-top:0.25rem" />
        </label>
        <label>
          重放批次大小
          <input v-model.number="batchSize" type="number" min="1" style="display:block;width:8rem;margin-top:0.25rem" />
        </label>
        <label>
          持锁秒数（互斥演示）
          <input v-model.number="holdSeconds" type="number" min="0" max="10" step="0.5" style="display:block;width:9rem;margin-top:0.25rem" />
        </label>
        <label style="display:flex;align-items:center;gap:0.4rem;margin-bottom:0.35rem">
          <input type="checkbox" v-model="forceFail" />
          注入失败（验证不污染主投影）
        </label>
        <button :disabled="busy || streamId == null" @click="startReplay">
          {{ busy ? '重放中…' : '从事件源重放' }}
        </button>
      </div>
      <p class="muted" style="margin-bottom:0">
        互斥演示：持锁秒数 &gt; 0 时，重放期间去「投影台」对同流发起开跑，将收到 409；
        反之在投影台带持锁开跑，再点本页重放同样被拒。不同流互不阻塞。
      </p>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">当前按流锁</h3>
      <table v-if="locks.length">
        <thead><tr><th>流</th><th>操作</th><th>持有者</th><th>获取时间</th><th>过期时间</th></tr></thead>
        <tbody>
          <tr v-for="l in locks" :key="l.stream_id">
            <td>{{ l.stream_id }}</td>
            <td><span class="tag warn">{{ l.operation }}</span></td>
            <td class="mono">{{ l.holder_ref }}</td>
            <td class="mono">{{ l.acquired_at }}</td>
            <td class="mono">{{ l.expires_at }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted" style="margin:0">无持锁；重放/开跑结束后锁必须释放。</p>
    </div>

    <div class="panel">
      <h3 style="margin-top:0">重放记录</h3>
      <table>
        <thead>
          <tr>
            <th>重放号</th><th>流</th><th>状态</th><th>目标 seq</th>
            <th>截断事件</th><th>源 max seq</th><th>批次数</th>
            <th>快照行(存/删)</th><th>快照 max seq</th><th>时间</th><th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in replays" :key="r.id">
            <td class="mono">{{ r.replay_no }}</td>
            <td>{{ r.stream_title }}</td>
            <td><span class="tag" :class="statusTag(r.status)">{{ r.status }}</span></td>
            <td class="mono">{{ r.target_seq }}</td>
            <td class="mono">{{ r.source_event_count }}</td>
            <td class="mono">{{ r.source_max_seq }}</td>
            <td class="mono">{{ r.batch_count }}</td>
            <td class="mono">{{ r.rows_total }} ({{ r.rows_alive }}/{{ r.rows_deleted }})</td>
            <td class="mono">{{ r.snapshot_max_seq }}</td>
            <td class="mono">{{ r.created_at }}</td>
            <td><button style="padding:0.25rem 0.6rem" @click="loadDetail(r.id)">查看</button></td>
          </tr>
          <tr v-if="!replays.length"><td colspan="11" class="muted">还没有重放记录。</td></tr>
        </tbody>
      </table>
    </div>

    <template v-if="detail">
      <div class="panel">
        <h3 style="margin-top:0">
          重放详情 <span class="mono">{{ detail.replay_no }}</span>
          <span class="tag" :class="statusTag(detail.status)" style="margin-left:0.5rem">{{ detail.status }}</span>
          <span v-if="detail.error" class="tag danger" style="margin-left:0.4rem">{{ detail.error }}</span>
        </h3>
        <div class="stat-grid">
          <div class="stat"><div class="stat-num">{{ detail.target_seq }}</div><div class="stat-label">目标 seq</div></div>
          <div class="stat"><div class="stat-num">{{ detail.source_event_count }}</div><div class="stat-label">截断点事件数</div></div>
          <div class="stat"><div class="stat-num">{{ detail.source_max_seq }}</div><div class="stat-label">源截断 max seq</div></div>
          <div class="stat"><div class="stat-num">{{ detail.batch_count }}</div><div class="stat-label">重放批次</div></div>
          <div class="stat"><div class="stat-num">{{ detail.rows_total }}</div><div class="stat-label">快照行</div></div>
          <div class="stat"><div class="stat-num">{{ detail.snapshot_max_seq }}</div><div class="stat-label">快照 max seq</div></div>
        </div>

        <h4>互证（截断点 × 批次计数 × 行集合 × max seq）</h4>
        <table v-if="detail.checks">
          <thead><tr><th>校验项</th><th>结果</th><th>说明</th></tr></thead>
          <tbody>
            <tr v-for="c in detail.checks.checks" :key="c.name">
              <td class="mono">{{ c.name }}</td>
              <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? '通过' : '不符' }}</span></td>
              <td class="muted">{{ c.detail }}</td>
            </tr>
          </tbody>
        </table>

        <h4>重放批次计数</h4>
        <table>
          <thead>
            <tr><th>#</th><th>seq 区间</th><th>源事件</th><th>写入</th><th>删除</th><th>跳过</th><th>批后存活</th><th>批后已删</th><th>批 max seq</th></tr>
          </thead>
          <tbody>
            <tr v-for="b in detail.batches" :key="b.id">
              <td class="mono">{{ b.batch_index }}</td>
              <td class="mono">{{ b.seq_from }}–{{ b.seq_to }}</td>
              <td>{{ b.source_events }}</td>
              <td>{{ b.applied_upsert }}</td>
              <td>{{ b.applied_delete }}</td>
              <td>{{ b.skipped_duplicate }}</td>
              <td>{{ b.projected_alive }}</td>
              <td>{{ b.projected_deleted }}</td>
              <td class="mono">{{ b.max_seq }}</td>
            </tr>
          </tbody>
        </table>

        <h4>目标快照行集合</h4>
        <table>
          <thead><tr><th>实体键</th><th>seq</th><th>事件</th><th>状态</th><th>载荷</th></tr></thead>
          <tbody>
            <tr v-for="row in detail.rows" :key="row.id">
              <td class="mono">{{ row.entity_key }}</td>
              <td class="mono">{{ row.applied_seq }}</td>
              <td class="mono">{{ row.applied_event_id }}</td>
              <td><span class="tag" :class="row.deleted ? 'danger' : 'ok'">{{ row.deleted ? '已删' : '存活' }}</span></td>
              <td class="mono">{{ row.payload == null ? '—' : JSON.stringify(row.payload) }}</td>
            </tr>
            <tr v-if="!detail.rows.length"><td colspan="5" class="muted">空快照（目标 seq 之前没有事件）。</td></tr>
          </tbody>
        </table>

        <button
          v-if="detail.status === 'completed'"
          @click="promote"
          :disabled="busy"
        >提升为主投影（独立接口，复核截断点后移动主水位）</button>
        <p v-else class="muted">
          状态 {{ detail.status }} 不可提升；仅互证通过的 completed 重放能走提升接口。
          <template v-if="detail.promoted_at">已于 {{ detail.promoted_at }} 提升为主批次 #{{ detail.promoted_batch_id }}。</template>
        </p>
      </div>

      <div class="panel" v-if="diff">
        <h3 style="margin-top:0">
          与主投影差异
          <span class="tag" :class="diff.identical ? 'ok' : 'warn'" style="margin-left:0.5rem">
            {{ diff.identical ? '一致' : '有差异' }}
          </span>
        </h3>
        <p class="muted">
          重放 max seq {{ diff.replay_max_seq }}（目标 {{ diff.target_seq }}） ·
          主水位 seq {{ diff.main_watermark_seq }} ·
          重放行 {{ diff.replay_rows }} / 主投影行 {{ diff.main_rows }}
        </p>
        <table v-if="diff.changed.length || diff.only_in_main.length || diff.only_in_replay.length">
          <thead><tr><th>类型</th><th>实体键</th><th>重放快照</th><th>主投影</th></tr></thead>
          <tbody>
            <tr v-for="x in diff.changed" :key="'c' + x.entity_key">
              <td><span class="tag warn">同行变更</span></td>
              <td class="mono">{{ x.entity_key }}</td>
              <td class="mono">seq {{ x.replay.applied_seq }} · {{ x.replay.deleted ? '已删' : '存活' }} · {{ JSON.stringify(x.replay.payload) }}</td>
              <td class="mono">seq {{ x.main.applied_seq }} · {{ x.main.deleted ? '已删' : '存活' }} · {{ JSON.stringify(x.main.payload) }}</td>
            </tr>
            <tr v-for="x in diff.only_in_main" :key="'m' + x.entity_key">
              <td><span class="tag danger">仅主投影</span></td>
              <td class="mono">{{ x.entity_key }}</td>
              <td class="muted">—</td>
              <td class="mono">seq {{ x.applied_seq }} · {{ x.deleted ? '已删' : '存活' }}</td>
            </tr>
            <tr v-for="x in diff.only_in_replay" :key="'r' + x.entity_key">
              <td><span class="tag ok">仅重放快照</span></td>
              <td class="mono">{{ x.entity_key }}</td>
              <td class="mono">seq {{ x.applied_seq }} · {{ x.deleted ? '已删' : '存活' }}</td>
              <td class="muted">—</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="muted" style="margin-bottom:0">行集合、max seq 与载荷完全一致。</p>
      </div>
    </template>
  </div>
</template>
