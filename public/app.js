// Ingest desk page. Vanilla ES module: polls the control API, renders the
// pipeline, per-global bands, ledgers, one datum's whole lifecycle, the merged
// event stream and the reconciliation against the supplier's manifest.

const api = (path) => fetch(path, { cache: 'no-store' }).then((r) => r.json());
const post = (path, body = {}) =>
  fetch(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }).then((r) => r.json());

const $ = (sel) => document.querySelector(sel);
const state = {
  ov: null,
  rec: null,
  trace: null,
  selected: null,
  follow: false,
  preset: 'steady',
  policy: 'overlap',
  eventFilter: 'all',
  ticks: 0,
  lastTraceAt: 0,
  busy: false,
};

// ------------------------------------------------------------------ helpers

function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') n.className = v;
    else if (k === 'text') n.textContent = v;
    else if (k.startsWith('on')) n.addEventListener(k.slice(2), v);
    else if (k === 'dataset') Object.assign(n.dataset, v);
    else n.setAttribute(k, v === true ? '' : String(v));
  }
  for (const kid of kids.flat(3)) {
    if (kid == null || kid === false) continue;
    n.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return n;
}

const nf = new Intl.NumberFormat('en-US');
const num = (n) => (n == null || Number.isNaN(n) ? '—' : nf.format(n));
const shortFp = (fp) => (fp ? String(fp).slice(0, 8) : '—');
const pct = (a, b) => (b ? Math.max(0, Math.min(100, (a / b) * 100)) : 0);

function fmtClock(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}.${String(d.getMilliseconds()).padStart(3, '0')}`;
}
function fmtShortClock(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')}`;
}
function rel(ms) {
  const start = state.ov?.source?.startedAt;
  if (!ms || !start) return '—';
  return `t+${((ms - start) / 1000).toFixed(2)}s`;
}
function qty(n) {
  return Number(n || 0).toLocaleString('en-US');
}
function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

// Tags carry an icon/dot plus a word, never colour alone.
function tag(text, tone = '') {
  return el('span', { class: `tag ${tone}` }, el('span', { class: 'dot', 'aria-hidden': 'true' }), text);
}
function chip(text, tone = '', live = false) {
  return el('span', { class: `chip ${tone} ${live ? 'live' : ''}` }, el('span', { class: 'dot' }), text);
}

// ------------------------------------------------------------------- tooltip

const tip = $('#tip');
function showTip(html, x, y) {
  tip.replaceChildren(...html);
  tip.classList.add('show');
  const pad = 14;
  const r = tip.getBoundingClientRect();
  let left = x + pad;
  let top = y + pad;
  if (left + r.width > window.innerWidth - 8) left = x - r.width - pad;
  if (top + r.height > window.innerHeight - 8) top = y - r.height - pad;
  tip.style.left = `${Math.max(8, left)}px`;
  tip.style.top = `${Math.max(8, top)}px`;
}
function hideTip() {
  tip.classList.remove('show');
}

function tipRow(k, v, hi = false) {
  return el('div', { class: 't-row' }, el('span', { class: 'k', text: k }), el('span', { class: `v ${hi ? 'hi' : ''}`, text: v }));
}

// ------------------------------------------------------------------ run meta

function renderRunMeta() {
  const ov = state.ov;
  const host = $('#runmeta');
  if (!ov) return;
  const run = ov.run || {};
  const src = ov.source || {};
  const rx = ov.receiver || {};
  clear(host);
  const chips = [
    chip(`预设 ${run.preset === 'chaos' ? '重连·补取·裁决' : '平稳'}`),
    chip(`策略 ${rx.policy === 'hold' ? '先停 hold' : '重叠 overlap'}`),
    chip(`总号 ${num(run.plan?.total)}`),
    chip(`已发布 ${num(src.published)}`, '', src.published < (src.total || 0)),
    chip(rx.receivedComplete ? '接收齐' : '接收中', rx.receivedComplete ? 'ok' : 'warn', !rx.receivedComplete),
    chip(rx.verifiedComplete ? '已验证完整' : '待确证', rx.verifiedComplete ? 'ok' : 'warn', !rx.verifiedComplete),
  ];
  if (rx.paused) chips.push(chip('已暂停', 'warn'));
  host.append(...chips);
}

// ---------------------------------------------------------------------- KPIs

function renderKpis() {
  const ov = state.ov;
  if (!ov) return;
  const rx = ov.receiver || {};
  const ld = ov.lander?.stats || {};
  const total = rx.total || 0;
  const covered = rx.covered || 0;
  const cursor = rx.cursor || 0;
  const gaps = rx.gaps || [];
  const tentative = rx.tentative || [];
  const conflicts = rx.conflicts || [];
  const pending = conflicts.filter((c) => c.decision === 'pending');
  const acked = ld.acked || 0;

  $('#kpi-recv-v').textContent = num(covered);
  $('#kpi-recv-t').textContent = `/ ${num(total)}`;
  $('#kpi-recv-bar').style.width = `${pct(covered, total)}%`;
  clear($('#kpi-recv-sub'));
  $('#kpi-recv-sub').append(
    el('span', { text: `连续游标 ${num(cursor)}` }),
    el('span', { class: gaps.length ? 'neg' : 'pos', text: `${gaps.length} 个空洞` }),
    el('span', { class: tentative.length ? 'neg' : 'pos', text: `${tentative.length} 个暂定` })
  );

  const verified = rx.verifiedComplete;
  $('#kpi-verified-v').textContent = `${num(cursor)} / ${num(total)}`;
  $('#kpi-verified-bar').style.width = `${pct(cursor, total)}%`;
  clear($('#kpi-verified-sub'));
  $('#kpi-verified-sub').append(
    el('span', { text: `firm ${num(covered - tentative.length)}` }),
    el('span', { class: pending.length ? 'neg' : 'pos', text: `待裁决 ${pending.length}` }),
    el('span', { class: tentative.length ? 'neg' : 'pos', text: `尾部猜测 ${tentative.length}` })
  );
  $('#kpi-verified').classList.toggle('warn', !verified && cursor > 0);

  $('#kpi-gaps-v').textContent = num(gaps.length);
  clear($('#kpi-gaps-sub'));
  $('#kpi-gaps-sub').append(
    el('span', { text: `累计开洞 ${num(rx.gapsOpened)}` }),
    el('span', { text: `已补 ${num(rx.gapsFilled)}` }),
    el('span', { class: rx.rejected ? 'neg' : '', text: `拒收 ${num(rx.rejected)}` })
  );

  $('#kpi-conf-v').textContent = num(conflicts.length);
  clear($('#kpi-conf-sub'));
  const byRule = conflicts.filter((c) => c.decided_by === 'rule' || c.decided_by === 'auto').length;
  const byBf = conflicts.filter((c) => c.decided_by === 'backfiller').length;
  $('#kpi-conf-sub').append(
    el('span', { class: pending.length ? 'neg' : 'pos', text: `待裁决 ${pending.length}` }),
    el('span', { text: `规则 ${byRule} · 补取 ${byBf}` })
  );

  $('#kpi-land-v').textContent = num(acked);
  $('#kpi-land-t').textContent = `/ ${num(total)}`;
  $('#kpi-land-bar').style.width = `${pct(acked, total)}%`;
  clear($('#kpi-land-sub'));
  $('#kpi-land-sub').append(
    el('span', { text: `游标 ${num(ld.ackedCursor)}` }),
    el('span', { class: ld.provisional ? 'neg' : 'pos', text: `暂定行 ${ld.provisional}` }),
    el('span', { text: `修订 ${num(ld.revisions)}` })
  );
}

// ------------------------------------------------------------------ pipeline

function renderPipeline() {
  const ov = state.ov;
  const host = $('#pipeline');
  if (!ov) return;
  const src = ov.source || {};
  const rx = ov.receiver || {};
  const bf = ov.backfiller || {};
  const ld = ov.lander || {};
  const lst = ld.stats || {};
  const conflicts = rx.conflicts || [];
  const pending = conflicts.filter((c) => c.decision === 'pending').length;

  const stages = [
    {
      name: '源端',
      role: 'fixture · :8899',
      tone: src.published >= (src.total || 0) ? 'ok' : 'ok',
      rows: [
        ['已发布', `${num(src.published)} / ${num(src.total)}`],
        ['长连接', num(src.stats?.liveLines)],
        ['补取会话', num(src.stats?.replaySessions)],
        ['尾部帧', num(src.stats?.tailFrames)],
        ['重编号 / 中断', `${num(src.stats?.renumbers)} / ${num(src.stats?.sessionBreaks)}`],
      ],
    },
    {
      name: '接收器',
      role: 'receiver.db',
      tone: (rx.gaps || []).length ? 'warn' : 'ok',
      rows: [
        ['游标', `${num(rx.cursor)} / ${num(rx.total)}`],
        ['覆盖', num(rx.covered)],
        ['空洞', num((rx.gaps || []).length)],
        ['隔离帧', num(rx.quarantine)],
        ['在线连接', num(rx.reconnects)],
      ],
    },
    {
      name: '缺口补取器',
      role: 'backfill.db',
      tone: bf.active ? 'ok' : bf.failures ? 'warn' : 'ok',
      rows: [
        ['当前会话', bf.active ? `${bf.active.from}–${bf.active.to}` : '空闲'],
        ['已补帧', num(bf.active?.frames ?? sum(bf.sessions, 'frames'))],
        ['完成会话', num((bf.sessions || []).filter((s) => s.state === 'complete').length)],
        ['失败 / 放弃', `${num(bf.failures)}`],
        ['轮次上限', '3'],
      ],
    },
    {
      name: '裁决',
      role: 'receiver.db · conflicts',
      tone: pending ? 'warn' : 'ok',
      rows: [
        ['冲突组', num(conflicts.length)],
        ['待裁决', num(pending)],
        ['已裁决', num(conflicts.length - pending)],
        ['规则', 'replay > live > tail'],
        ['窗口', '1.4 s'],
      ],
    },
    {
      name: '入库器',
      role: 'lander.db',
      tone: ld.ok ? (lst.committed || lst.staged ? 'warn' : 'ok') : 'bad',
      rows: [
        ['已确认', `${num(lst.acked)} / ${num(rx.total)}`],
        ['批次', num(lst.batches)],
        ['修订', num(lst.revisions)],
        ['暂定行', num(lst.provisional)],
        ['崩溃恢复', num((ld.recovered || []).length)],
      ],
    },
  ];

  clear(host);
  for (const s of stages) {
    host.append(
      el(
        'div',
        { class: `stage ${s.tone}` },
        el('div', { class: 'name' }, el('span', { class: 'dot' }), s.name),
        el('div', { class: 'role', text: s.role }),
        el(
          'dl',
          {},
          s.rows.flatMap(([k, v]) => [el('dt', { text: k }), el('dd', {}, el('b', { text: String(v) }))])
        )
      )
    );
  }
}
function sum(arr, key) {
  return (arr || []).reduce((a, b) => a + (Number(b[key]) || 0), 0);
}

// ----------------------------------------------------------- completeness

function renderChecklist() {
  const ov = state.ov;
  const rec = state.rec;
  if (!ov) return;
  const rx = ov.receiver || {};
  const ld = ov.lander?.stats || {};
  const total = rx.total || 0;
  const checks = [
    {
      ok: total > 0 && rx.cursor >= total,
      what: '业务顺序连续：游标 1 → 总数',
      why: `receiver.cursor = ${num(rx.cursor)}`,
      state: `${num(rx.cursor)}/${num(total)}`,
    },
    {
      ok: total > 0 && (rx.gaps || []).length === 0,
      what: '无空洞：每个号都有内容',
      why: 'gaps 表里没有未被覆盖的号',
      state: `${num((rx.gaps || []).length)} 洞`,
    },
    {
      ok: total > 0 && (rx.tentative || []).length === 0,
      what: '全部 firm：没有仅由迟到尾部支撑的号',
      why: '尾部帧只算暂定，必须被按序号补取确证',
      state: `${num((rx.tentative || []).length)} 暂定`,
    },
    {
      ok: total > 0 && (rx.pendingConflicts || []).length === 0,
      what: '无待裁决冲突',
      why: '冲突组全部有记录在案的胜者',
      state: `${num((rx.pendingConflicts || []).length)} 待定`,
    },
    {
      ok: total > 0 && ld.acked === total,
      what: '落盘：每一行都已 acked',
      why: 'acked 表示批次已提交且记为进度',
      state: `${num(ld.acked)}/${num(total)}`,
    },
    {
      ok: total > 0 && (ld.provisional || 0) === 0,
      what: '落盘行无暂定：没有基于未裁决内容的行',
      why: 'provisional=1 的行已落盘但内容尚未定案',
      state: `${num(ld.provisional)} 行`,
    },
    {
      ok: !!rec && rec.mismatchCount === 0,
      what: '逐号与源端清单一致',
      why: rec ? `mismatch = ${num(rec.mismatchCount)}` : '尚未对账',
      state: rec ? (rec.mismatchCount === 0 ? '一致' : `${num(rec.mismatchCount)} 处`) : '未对账',
    },
    {
      ok: !!rec && rec.fingerprintMatch === true,
      what: '全流指纹一致：同一函数的同一结果',
      why: '两端都用 manifestFp(global, fp) 计算',
      state: rec ? (rec.fingerprintMatch ? '相同' : '不同') : '未对账',
    },
  ];
  const host = $('#checklist');
  clear(host);
  for (const c of checks) {
    host.append(
      el(
        'div',
        { class: `check ${c.ok ? 'pass' : 'fail'}` },
        el('span', { class: 'icon', text: c.ok ? '✓' : '✗' }),
        el('span', { class: 'what' }, c.what, el('div', { class: 'why', text: c.why })),
        el('span', { class: 'state', text: c.state })
      )
    );
  }
  const all = checks.every((c) => c.ok);
  const missing = checks.filter((c) => !c.ok);
  const vh = $('#verdict-host');
  clear(vh);
  vh.append(
    el(
      'div',
      { class: `verdict ${all ? 'pass' : ''}` },
      el('span', { class: 'icon', text: all ? '✓' : '…' }),
      el(
        'span',
        {},
        all ? '足以确认完整：接收、确证、裁决、落盘与源端清单全部一致。' : '尚不足以确认完整：',
        all
          ? null
          : el('span', {
              class: 'why',
              text: missing.map((m) => m.what.split('：')[0]).join('；'),
            })
      )
    )
  );
}

// ---------------------------------------------------------------- matrix

const stateOfGlobal = (g) => {
  const ov = state.ov;
  if (!ov) return null;
  const rx = ov.receiver || {};
  const ld = ov.lander || {};
  const decisions = ov.ledger?.decisions || [];
  const landed = ov.ledger?.landed || [];
  const d = decisions[g - 1];
  const l = landed[g - 1];
  const gap = (rx.gaps || []).find((x) => x.global === g);
  const tent = (rx.tentative || []).find((x) => x.global === g);
  const conf = (rx.conflicts || []).find((x) => x.global === g);
  return {
    d,
    l,
    gap,
    tent,
    conf,
    covered: !!d,
    grade: tent ? 'tentative' : d ? 'firm' : gap ? 'gap' : 'pending',
    origin: d?.origin ?? null,
    landState: l ? (l.provisional ? 'provisional' : l.acked ? 'acked' : 'unlanded') : 'unlanded',
  };
};

function renderMatrix() {
  const ov = state.ov;
  if (!ov) return;
  const total = ov.receiver?.total || ov.run?.plan?.total || 0;
  if (!total) return;

  // Legend: colour plus a word, so identity never depends on hue alone.
  const lg = $('#matrix-legend');
  clear(lg);
  const items = [
    ['firm', 'firm 已确证'],
    ['tentative', 'tentative 尾部猜测'],
    ['gap', '空洞 / 未覆盖'],
    ['replay', 'replay 补取来源'],
    ['conflict', '冲突（实心=已裁决，空心=待裁决）'],
  ];
  for (const [k, label] of items) {
    lg.append(el('span', { class: 'item' }, el('span', { class: `swatch ${k}` }), label));
  }

  const wrap = $('#matrix-wrap');
  clear(wrap);

  // Per-epoch rails first: which socket/epoch owns which span of the order.
  // This is the "identity" band — it says nothing about coverage.
  const rails = el('div', { class: 'epoch-rails' });
  const conns = (ov.receiver?.connections || []).filter((c) => c.global_first != null);
  for (const c of conns) {
    const from = Math.max(1, c.global_first);
    const to = Math.min(total, c.global_last ?? c.global_first);
    const left = ((from - 1) / total) * 100;
    const width = Math.max(0.4, ((to - from + 1) / total) * 100);
    const tailed = (ov.receiver?.origins?.tail || 0) > 0 && c.role === 'live' && c.state === 'closed';
    rails.append(
      el(
        'div',
        { class: 'epoch-rail' },
        el('span', { class: 'who' }, c.conn_id, el('em', { text: ` e${c.epoch ?? '—'}` })),
        el(
          'div',
          { class: 'track' },
          el('i', {
            class: c.role === 'repair' ? '' : tailed ? 'tail' : 'live',
            style: `left:${left}%;width:${width}%`,
            title: `${c.conn_id} · epoch ${c.epoch} · ${from}–${to}`,
          })
        ),
        el('span', { class: 'span', text: `${from}–${to}${c.role === 'repair' ? ' 补取' : ''}` })
      )
    );
  }
  wrap.append(rails);

  wrap.append(
    matrixRow('接收覆盖', 'receiver.db · coverage', total, (g) => {
      const s = stateOfGlobal(g);
      let cls = 'cell';
      if (s.grade === 'firm') cls += ' firm';
      else if (s.grade === 'tentative') cls += ' tentative';
      else if (s.grade === 'gap') cls += ' gap';
      else cls += ' pending';
      if (s.origin === 'replay') cls += ' is-replay';
      if (s.conf) cls += s.conf.decision === 'pending' ? ' is-pending' : ' is-conflict';
      if (state.selected === g) cls += ' selected';
      return el('div', {
        class: cls,
        dataset: { g, band: 'coverage' },
        role: 'button',
        tabindex: '0',
        'aria-label': `序号 ${g} 接收覆盖 ${s.grade}`,
      });
    }),
    matrixRow('来源路径', 'frames.origin · identity', total, (g) => {
      const s = stateOfGlobal(g);
      let cls = 'cell';
      if (!s.covered) cls += ' pending';
      else cls += ' firm';
      if (s.origin === 'replay') cls += ' is-replay';
      else if (s.origin === 'tail') cls += ' is-tail';
      if (state.selected === g) cls += ' selected';
      return el('div', {
        class: cls,
        dataset: { g, band: 'origin' },
        role: 'button',
        tabindex: '0',
        'aria-label': `序号 ${g} 来源 ${s.origin || '无'}`,
      });
    }),
    matrixRow('落盘进度', 'lander.db · landed', total, (g) => {
      const s = stateOfGlobal(g);
      let cls = 'cell';
      if (s.landState === 'acked') cls += ' acked';
      else if (s.landState === 'provisional') cls += ' provisional';
      else cls += ' unlanded';
      if (s.l && s.l.revision > 0) cls += ' is-amended';
      if (state.selected === g) cls += ' selected';
      return el('div', {
        class: cls,
        dataset: { g, band: 'landed' },
        role: 'button',
        tabindex: '0',
        'aria-label': `序号 ${g} 落盘 ${s.landState}`,
      });
    })
  );
  const foot = el('div', { class: 'matrix-foot' });
  foot.append(
    el('span', { text: '1' }),
    el('span', { text: `每格 = 一个业务序号 · 共 ${num(total)} 号 · 悬停查看，点击查看全程` }),
    el('span', { text: num(total) })
  );
  wrap.append(foot);

  renderMatrixTables(total);
}

function matrixRow(label, sub, total, cellFn) {
  // Wrap the order into lines of at most ~64 cells: 222 one-pixel-wide cells in
  // a single row is a strip, not a readable matrix.
  const perLine = 64;
  const lines = el('div', { class: 'matrix-lines' });
  for (let start = 1; start <= total; start += perLine) {
    const end = Math.min(total, start + perLine - 1);
    const line = el('div', { class: 'matrix-line', style: `--cols:${end - start + 1}` });
    line.append(el('span', { class: 'gutter', text: String(start) }));
    for (let g = start; g <= end; g++) line.append(cellFn(g));
    lines.append(line);
  }
  return el(
    'div',
    { class: 'matrix-row' },
    el('div', { class: 'matrix-label' }, el('b', { text: label }), el('span', { text: sub })),
    lines
  );
}

function renderMatrixTables(total) {
  const body = $('#matrix-tables-body');
  if (!body) return;
  clear(body);
  const s = state.ov?.source || {};
  const rx = state.ov?.receiver || {};
  const ld = state.ov?.lander?.stats || {};
  const rows = [
    ['接收覆盖', 'receiver.db', `${num(rx.covered)} 号有内容`],
    ['firm', 'coverage.grade', `${num((rx.covered || 0) - (rx.tentative || []).length)} 号`],
    ['tentative', 'coverage.grade', `${num((rx.tentative || []).length)} 号`],
    ['空洞', 'gaps', `${num((rx.gaps || []).length)} 号`],
    ['冲突组', 'conflicts', `${num((rx.conflicts || []).length)} 组`],
    ['落盘已确认', 'landed.acked=1', `${num(ld.acked)} 号`],
    ['落盘暂定', 'landed.provisional=1', `${num(ld.provisional)} 号`],
    ['修订', 'landed.revision', `${num(ld.revisions)} 次`],
    ['来源 live', 'frames.origin', `${num(rx.origins?.live)} 帧`],
    ['来源 replay', 'frames.origin', `${num(rx.origins?.replay)} 帧`],
    ['来源 tail', 'frames.origin', `${num(rx.origins?.tail)} 帧`],
    ['源端已发布', 'fixture clock', `${num(s.published)} 号`],
  ];
  body.append(
    el(
      'table',
      { class: 'tbl' },
      el('caption', { text: '三个库的同一批号，各自给出独立答案' }),
      el(
        'thead',
        {},
        el('tr', {}, el('th', { text: '项' }), el('th', { text: '来源表' }), el('th', { text: '数量' }))
      ),
      el(
        'tbody',
        {},
        rows.map(([a, b, c]) =>
          el('tr', {}, el('td', { text: a }), el('td', { class: 'muted', text: b }), el('td', { class: 'num', text: c }))
        )
      )
    )
  );
}

// ------------------------------------------------------------- connections

function renderLines() {
  const host = $('#lines');
  const rx = state.ov?.receiver || {};
  if (!host) return;
  clear(host);
  const conns = (rx.connections || []).filter((c) => c.role === 'live');
  for (const c of conns) {
    const line = (rx.lines || []).find((l) => l.connId === c.conn_id) || {};
    const tone =
      line.state === 'live' ? 'live' : line.state === 'suspect' || line.state === 'degraded' ? 'suspect' : 'closed';
    host.append(
      el(
        'div',
        { class: `line-card ${tone}` },
        el('div', { class: 'id' }, el('span', { class: 'dot' }), c.conn_id),
        el('div', { class: 'meta', text: `epoch ${c.epoch ?? '—'} · ${c.label || line.state || '—'}` }),
        el('div', { class: 'meta', text: `${num(c.global_first)} → ${num(c.global_last)} · ${num(c.frames_accepted)} 帧` }),
        line.supersededBy === 'paused'
          ? el('div', { class: 'meta super', text: '暂停接管' })
          : line.supersededBy
          ? el('div', { class: 'meta super', text: `已被 ${line.supersededBy} 接管` })
          : null
      )
    );
  }
  if (!conns.length) host.append(el('div', { class: 'callout' }, '尚未建立连接。'));
}

function renderConnTable() {
  const rx = state.ov?.receiver || {};
  const tbody = $('#conn-table tbody');
  clear(tbody);
  const rows = [...(rx.connections || [])].sort((a, b) => a.opened_at - b.opened_at);
  for (const c of rows) {
    const basis = c.basis;
    const map = basis ? `${basis.releaseStart} → ${basis.globalStart}` : '未声明（隔离）';
    const bad = !basis;
    tbody.append(
      el(
        'tr',
        {},
        el('td', { class: 'num clickable', text: c.conn_id, onclick: () => openTrace(c.global_last || c.global_first) }),
        el('td', {}, c.role === 'live' ? tag('live 长连接', 'info') : tag('repair 补取', 'replay')),
        el('td', { class: 'num', text: c.epoch ?? '—' }),
        el('td', { class: bad ? 'muted' : 'num', text: map }),
        el('td', { class: 'num', text: `${num(c.global_first)}–${num(c.global_last)}` }),
        el('td', { class: 'num', text: num(c.frames_accepted) }),
        el('td', {}, tag(c.label || c.state, stateTone(c.state)))
      )
    );
  }
  if (!rows.length) tbody.append(el('tr', {}, el('td', { colspan: '7', class: 'muted', text: '无连接' })));
}

function stateTone(s) {
  if (['live', 'complete', 'decided'].includes(s)) return 'ok';
  if (['suspect', 'degraded', 'requested', 'active', 'pending'].includes(s)) return 'warn';
  if (['aborted', 'failed', 'abandoned'].includes(s)) return 'bad';
  return 'mute';
}

function renderSessions() {
  const bf = state.ov?.backfiller || {};
  const tbody = $('#session-table tbody');
  clear(tbody);
  const rows = bf.sessions || [];
  for (const s of rows) {
    tbody.append(
      el(
        'tr',
        {},
        el('td', { class: 'num', text: s.session_id }),
        el('td', { class: 'num', text: `${s.from_global}–${s.to_global}` }),
        el('td', { class: 'num', text: `#${s.attempt}` }),
        el('td', {}, s.purpose === 'verify' ? tag('确证', 'warn') : tag('补缺口', 'info')),
        el('td', { class: 'num', text: num(s.frames) }),
        el('td', {}, tag(s.label || s.state, stateTone(s.state))),
        el('td', { class: 'muted wrap', text: s.note || '—' })
      )
    );
  }
  if (!rows.length) {
    tbody.append(el('tr', {}, el('td', { colspan: '7', class: 'muted', text: '没有补取会话' })));
  }
  const note = $('#session-note');
  clear(note);
  const aborted = rows.filter((s) => s.state === 'aborted').length;
  const incomplete = rows.filter((s) => s.state === 'incomplete').length;
  if (aborted || incomplete) {
    note.className = 'callout';
    note.append(
      document.createTextNode(
        `源端说“完成”只是声明：${incomplete} 个会话结束后接收端仍有缺号，${aborted} 个会话中途被源端重置。补取器按区间重开，最多 3 轮。`
      )
    );
  } else {
    note.className = 'callout ok';
    note.append(document.createTextNode('每次补取结论都回到接收端核对覆盖，而不是相信源端的“完成”通知。'));
  }
}

function renderCosts() {
  const rx = state.ov?.receiver || {};
  const bf = state.ov?.backfiller || {};
  const o = rx.origins || {};
  const sets = (bf.sessions || []).length;
  const line = (host, active) => {
    clear(host);
    if (!active) {
      host.append(
        el('span', { class: 'pair' }, el('span', { class: 'k', text: '尚未在本策略下运行' })),
        el('span', { class: 'pair' }, el('span', { class: 'k', text: '切换后点击“重跑”即可采集成本' }))
      );
      return;
    }
    const pairs = [
      ['live 帧', num(o.live)],
      ['replay 帧', num(o.replay)],
      ['tail 帧', num(o.tail)],
      ['拒收帧', num(rx.rejected)],
      ['补取会话', num(sets)],
      ['补洞', `${num(rx.gapsFilled)}/${num(rx.gapsOpened)}`],
    ];
    for (const [k, v] of pairs) {
      host.append(el('span', { class: 'pair' }, el('span', { class: 'k', text: k }), el('span', { class: 'v hi', text: v })));
    }
  };
  line($('#cost-overlap'), rx.policy === 'overlap');
  line($('#cost-hold'), rx.policy === 'hold');
  const pn = $('#policy-note');
  clear(pn);
  pn.className = 'callout';
  pn.append(
    document.createTextNode(
      rx.policy === 'hold'
        ? 'hold：旧连接先被退休，它的迟到尾部按身份规则全部拒收；每个洞都得由按序号补取填上。数字上表现为 replay 帧多、tail 帧为 0。'
        : 'overlap：旧连接降级后立刻由新连接接管，旧连接的迟到尾部被当作暂定内容收下，再用补取确证。数字上表现为 tail 帧多、补取区间更小。'
    )
  );
}

// ------------------------------------------------------------------- events

function eventSummary(e) {
  const d = e.detail || {};
  const bits = [];
  if (d.state) bits.push(d.state);
  if (d.reason) bits.push(d.reason);
  if (d.epoch != null) bits.push(`epoch ${d.epoch}`);
  if (d.release != null) bits.push(`release ${d.release}`);
  if (d.groupId) bits.push(d.groupId);
  if (d.sessionId) bits.push(d.sessionId);
  if (d.from != null && d.to != null) bits.push(`${d.from}–${d.to}`);
  if (d.items != null) bits.push(`${d.items} 项`);
  if (d.verdict) bits.push(d.verdict);
  if (d.by) bits.push(`by ${d.by}`);
  if (d.policy) bits.push(d.policy);
  if (d.mode) bits.push(d.mode);
  if (d.rule) bits.push(d.rule);
  if (d.note) bits.push(d.note);
  if (d.from && d.to && typeof d.from === 'string') bits.push(`${shortFp(d.from)} → ${shortFp(d.to)}`);
  if (d.why) bits.push(d.why);
  if (d.kind) bits.push(d.kind);
  if (d.batchId) bits.push(String(d.batchId));
  if (d.revision != null) bits.push(`r${d.revision}`);
  if (d.missing != null || d.divergent != null) bits.push(`缺 ${d.missing ?? 0} · 异 ${d.divergent ?? 0}`);
  if (d.winner?.origin) bits.push(`胜者 ${d.winner.origin} ${shortFp(d.winner.fp)}`);
  if (d.held || d.challenger) bits.push('两变体已封存');
  if (d.ackedCursor != null) bits.push(`acked 游标 ${d.ackedCursor}`);
  if (d.count != null) bits.push(`${d.count}`);
  return bits.join(' · ');
}

function renderEvents() {
  const host = $('#events');
  const ov = state.ov;
  if (!ov) return;
  const events = (ov.events || []).slice().reverse();
  const f = state.eventFilter;
  const shown = f === 'all' ? events : events.filter((e) => e.actor === f);
  clear(host);
  for (const e of shown) {
    const g = e.global != null ? el('span', { class: 'g', text: `#${e.global}`, onclick: () => openTrace(e.global) }) : null;
    host.append(
      el(
        'div',
        { class: `event actor-${e.actor}` },
        el('span', { class: 't', text: fmtShortClock(e.at) }),
        el('span', { class: 'type', text: e.type }),
        el('span', { class: 'detail' }, g ? [g, ' '] : null, eventSummary(e))
      )
    );
  }
  if (!shown.length) host.append(el('div', { class: 'callout' }, '暂无事件。'));
}

// ---------------------------------------------------------------- reconcile

function renderReconcile() {
  const host = $('#reconcile-host');
  const rec = state.rec;
  const chipHost = $('#chip-reconcile');
  const chip2 = $('#chip-rec2');
  if (!host) return;
  if (!rec || rec.ok === false) {
    clear(host);
    host.append(el('div', { class: 'callout' }, rec?.error ? `对账失败：${rec.error}` : '尚未对账。'));
    return;
  }
  clear(host);
  const ok = rec.complete;
  host.append(
    el(
      'div',
      { class: 'rec-top' },
      recStat('源端总数', num(rec.total)),
      recStat('落盘', num(rec.landed)),
      recStat('已确认', num(rec.acked)),
      recStat('暂定', num(rec.provisional)),
      recStat('修订', num(rec.revisions)),
      recStat('落盘游标', num(rec.cursor)),
      el('span', { class: 'spacer' }),
      el('div', {}, el('span', { class: `chip ${ok ? 'ok' : 'bad'}` }, el('span', { class: 'dot' }), ok ? '完全一致' : '存在差异'))
    ),
    el(
      'div',
      { class: 'fp-compare' },
      el(
        'div',
        { class: `fp-box ${rec.fingerprintMatch ? 'match' : 'miss'}` },
        el('div', { class: 'k', text: '源端 manifestFp' }),
        el('div', { class: 'v', text: rec.manifestFp })
      ),
      el(
        'div',
        { class: `fp-box ${rec.fingerprintMatch ? 'match' : 'miss'}` },
        el('div', { class: 'k', text: '落盘重算 manifestFp' }),
        el('div', { class: 'v', text: rec.landedFp })
      )
    )
  );
  if (rec.mismatch && rec.mismatch.length) {
    const tbl = el(
      'table',
      { class: 'tbl' },
      el(
        'thead',
        {},
        el('tr', {}, el('th', { text: '序号' }), el('th', { text: '类型' }), el('th', { text: '期望' }), el('th', { text: '实际' }))
      ),
      el(
        'tbody',
        {},
        rec.mismatch.slice(0, 12).map((m) =>
          el(
            'tr',
            {},
            el('td', { class: 'num clickable', text: m.global, onclick: () => openTrace(m.global) }),
            el('td', {}, tag(m.kind, 'bad')),
            el('td', { class: 'num muted', text: shortFp(m.expected) }),
            el('td', { class: 'num muted', text: shortFp(m.actual) })
          )
        )
      )
    );
    host.append(el('div', { class: 'scroll-y', style: 'max-height:150px;margin-top:8px' }, tbl));
  } else {
    host.append(
      el(
        'div',
        { class: 'callout ok', style: 'margin-top:8px' },
        `逐号一致：${num(rec.total)} 号内容指纹全部相同，全流指纹相同；所有行已 acked，无暂定行。`
      )
    );
  }
  if (chipHost) {
    clear(chipHost);
    chipHost.className = `chip ${ok ? 'ok' : 'bad'}`;
    chipHost.append(el('span', { class: 'dot' }), el('span', { text: ok ? '清单对账一致' : `对账差异 ${num(rec.mismatchCount)}` }));
  }
  if (chip2) {
    clear(chip2);
    chip2.className = `chip ${ok ? 'ok' : 'bad'}`;
    chip2.append(el('span', { class: 'dot' }), el('span', { text: ok ? 'complete' : 'incomplete' }));
  }
}
function recStat(k, v) {
  return el('div', { class: 'rec-stat' }, el('div', { class: 'k', text: k }), el('div', { class: 'v', text: v }));
}

// -------------------------------------------------------------------- trace

function openTrace(g) {
  if (!Number.isFinite(Number(g)) || !g) return;
  state.selected = Number(g);
  state.follow = false;
  syncFollowButton();
  $('#trace-input').value = String(g);
  loadTrace();
  renderMatrix();
  document.querySelector('#trace-host')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

async function loadTrace() {
  const g = state.selected;
  if (!g) return;
  state.lastTraceAt = Date.now();
  try {
    state.trace = await api(`/api/trace/${g}`);
  } catch (err) {
    state.trace = { error: err.message };
  }
  renderTrace();
}

function renderTrace() {
  const host = $('#trace-host');
  const tr = state.trace;
  if (!tr) return;
  if (tr.error) {
    clear(host);
    host.append(el('div', { class: 'callout bad', text: `读取失败：${tr.error}` }));
    return;
  }
  const g = tr.global;
  const rx = tr.receiver || {};
  const cov = rx.coverage;
  const frames = rx.frames || [];
  const conf = rx.conflict;
  const land = tr.lander?.land || null;
  const manifest = tr.manifest;
  const live = frames.filter((f) => f.origin !== 'replay');
  const replays = frames.filter((f) => f.origin === 'replay');
  const quote = frames[0]?.payload || null;
  const session = findSessionFor(g, replays);

  clear(host);

  const head = el('div', { class: 'trace-head' });
  head.append(
    el('span', { class: 'gid', text: `#${num(g)}` }),
    quote ? el('span', { class: 'sym', text: quote.symbol }) : null,
    quote
      ? el(
          'span',
          { class: 'quote' },
          `tick ${quote.tick} · `,
          el('b', { text: Number(quote.px).toFixed(4) }),
          ` × ${qty(quote.qty)} · ${quote.side}`
        )
      : el('span', { class: 'muted', text: '接收端尚无内容' }),
    el('span', { class: 'muted', text: `fp ${shortFp(cov?.fp || manifest?.fp)}` }),
    el(
      'span',
      { class: 'verdict-badge' },
      cov
        ? tag(cov.grade === 'firm' ? 'firm 已确证' : 'tentative 暂定', cov.grade === 'firm' ? 'ok' : 'warn')
        : tag('无覆盖', 'bad'),
      land
        ? land.acked && !land.provisional
          ? tag('已落盘确认', 'ok')
          : land.provisional
          ? tag('落盘暂定', 'warn')
          : tag('已提交未确认', 'warn')
        : tag('未落盘', 'mute')
    )
  );
  host.append(head);

  const steps = el('div', { class: 'steps' });

  // 1 — receive
  const recvState = live.length ? (live.every((f) => f.origin === 'tail') ? 'warn' : 'done') : 'none';
  const recvBody = el('div', { class: 'body' });
  if (live.length) {
    for (const f of live) {
      recvBody.append(
        el(
          'div',
          { class: 'frame-row' },
          el('span', { class: 'conn', text: f.conn_id }),
          el('span', { class: 'desc' }, `epoch ${f.epoch} · release ${f.release} · `, el('b', { text: f.origin }), ` · ${shortFp(f.fp)}`),
          tag(
            f.decision === 'accepted' ? '首收' : f.decision === 'confirmed' ? '复述一致' : f.decision === 'rejected' ? '落选' : '争议',
            f.decision === 'rejected' ? 'mute' : f.decision === 'contested' ? 'warn' : 'mute'
          ),
          el('span', { class: 'muted', text: fmtClock(f.at) })
        )
      );
    }
  } else {
    recvBody.append(el('div', { class: 'none-note', text: '接收端没有这一号的任何帧 —— 它要么仍是空洞，要么只能由补取填上。' }));
  }
  steps.append(stepRow('接收', rel(frames.find((f) => f.origin !== 'replay')?.at), `${live.length} 帧`, recvBody, recvState));

  // 2 — backfill
  const bfBody = el('div', { class: 'body' });
  if (replays.length) {
    for (const f of replays) {
      bfBody.append(
        el(
          'div',
          { class: 'frame-row' },
          el('span', { class: 'conn', text: f.conn_id }),
          el('span', { class: 'desc' }, `epoch ${f.epoch} · release ${f.release} · `, el('b', { text: 'replay' }), ` · ${shortFp(f.fp)}`),
          tag(
            f.decision === 'confirmed' ? '确证一致' : f.decision === 'accepted' ? '补入' : f.decision === 'rejected' ? '落选' : '争议',
            f.decision === 'rejected' ? 'mute' : f.decision === 'contested' ? 'warn' : 'ok'
          ),
          el('span', { class: 'muted', text: fmtClock(f.at) })
        )
      );
    }
    const kv = el('div', { class: 'kv' });
    if (session) {
      kv.append(
        kvPair('会话', session.session_id),
        kvPair('区间', `${session.from_global}–${session.to_global}`),
        kvPair('轮次', `#${session.attempt}`),
        kvPair('用途', session.purpose === 'verify' ? '确证暂定' : '补缺口'),
        kvPair('源端声明', session.state === 'complete' ? 'complete' : session.state),
        kvPair('接收端结论', session.label || session.state),
        kvPair('备注', session.note || '—')
      );
    }
    bfBody.append(kv);
  } else {
    const gapEver = (tr.receiver?.events || []).some((e) => e.type.startsWith('gap.'));
    bfBody.append(
      el('div', {
        class: 'none-note',
        text: gapEver
          ? '这一号曾有洞，但补取与尾部一致，无需另开帧。'
          : '无需补取：接收端首收即为 firm，或有 live 帧直接覆盖。',
      })
    );
  }
  const bfTone = replays.length ? 'done' : 'none';
  steps.append(stepRow('补取', rel(replays[0]?.at), replays.length ? `${replays.length} 帧` : '不需要', bfBody, bfTone));

  // 3 — arbitration
  const arbBody = el('div', { class: 'body' });
  let arbTone = 'none';
  let arbWhen = '—';
  if (conf) {
    arbTone = conf.decision === 'pending' ? 'warn' : 'done';
    arbWhen = rel(conf.decided_at || conf.opened_at);
    const variants = frames.slice().sort((a, b) => rank(b) - rank(a));
    const tbl = el(
      'table',
      { class: 'tbl' },
      el(
        'thead',
        {},
        el('tr', {}, el('th', { text: '来源' }), el('th', { text: '连接 / epoch' }), el('th', { text: '内容' }), el('th', { text: '等级' }), el('th', { text: '裁决' }))
      ),
      el(
        'tbody',
        {},
        variants.map((f) =>
          el(
            'tr',
            {},
            el('td', {}, tag(f.origin, f.origin === 'replay' ? 'replay' : f.origin === 'tail' ? 'warn' : 'info')),
            el('td', { class: 'num', text: `${f.conn_id} · ${f.epoch}` }),
            el('td', { class: 'num', text: `${Number(f.payload.px).toFixed(4)} × ${qty(f.payload.qty)} · ${shortFp(f.fp)}` }),
            el('td', { class: 'num', text: `class ${rank(f)}` }),
            el(
              'td',
              {},
              f.fp === conf.winner_fp ? tag('胜出', 'ok') : tag('落选', 'mute')
            )
          )
        )
      )
    );
    arbBody.append(tbl);
    const kv = el('div', { class: 'kv' });
    kv.append(
      kvPair('规则', 'replay > live > tail'),
      kvPair('结论', conf.decision === 'pending' ? '等待裁决窗口' : `由 ${conf.decided_by || '—'} 裁决`),
      kvPair('胜者帧', `#${conf.winner_frame}`),
      kvPair('组', conf.group_id)
    );
    arbBody.append(kv);
    if (conf.decision === 'pending') {
      arbBody.append(el('div', { class: 'callout', text: '冲突已封存，等待来源等级或内容给出唯一胜者；此时落盘行会标记为暂定。' }));
    }
  } else {
    arbBody.append(
      el('div', {
        class: 'none-note',
        text: frames.length > 1
          ? '多个来源给出了相同内容指纹，无需裁决。'
          : frames.length === 1
          ? '只有一个来源，无冲突。'
          : '没有帧，无法裁决。',
      })
    );
  }
  steps.append(stepRow('裁决', arbWhen, conf ? '1 组' : '无冲突', arbBody, arbTone, conf?.decision === 'decided' ? 'arb' : ''));

  // 4 — landing
  const landBody = el('div', { class: 'body' });
  let landTone = 'none';
  if (land) {
    landTone = land.acked && !land.provisional ? 'done' : 'warn';
    const kv = el('div', { class: 'kv' });
    kv.append(
      kvPair('批次', land.batch_id),
      kvPair('批次状态', land.batch?.state || '—'),
      kvPair('行 acked', land.acked ? '是' : '否'),
      kvPair('行暂定', land.provisional ? '是' : '否'),
      kvPair('修订', `r${land.revision}`),
      kvPair('落盘时间', fmtClock(land.landed_at)),
      kvPair('当前指纹', shortFp(land.fp))
    );
    landBody.append(kv);
    if (land.superseded_fp) {
      landBody.append(
        el(
          'div',
          { class: 'callout warn' },
          `此行被修订：旧内容 ${shortFp(land.superseded_fp)} → 新内容 ${shortFp(land.fp)}（${land.amendments?.length || 0} 条修订记录）`
        )
      );
    }
    if (manifest) {
      const match = manifest.fp === land.fp;
      landBody.append(
        el(
          'div',
          { class: `callout ${match ? 'ok' : 'bad'}` },
          match ? `与源端清单一致：${shortFp(manifest.fp)}` : `与源端清单不同：期望 ${shortFp(manifest.fp)}，实际 ${shortFp(land.fp)}`
        )
      );
    }
  } else {
    landBody.append(
      el('div', { class: 'none-note', text: '尚未落盘：入库器按批次拉取接收端的判定，还没轮到这一号。' })
    );
  }
  steps.append(stepRow('落盘', rel(land?.landed_at), land ? `批次 ${land.batch_id}` : '未落盘', landBody, landTone, 'land'));

  host.append(steps);

  // Source-side events and receiver events that mention this global.
  const srcEvents = tr.sourceEvents || [];
  const rxEvents = rx.events || [];
  if (srcEvents.length || rxEvents.length) {
    const t = el(
      'table',
      { class: 'tbl', style: 'margin-top:10px' },
      el('caption', { text: '这一号在本进程之外留下的记录' }),
      el('thead', {}, el('tr', {}, el('th', { text: '时间' }), el('th', { text: '来源' }), el('th', { text: '事件' }), el('th', { text: '细节' }))),
      el(
        'tbody',
        {},
        [
          ...srcEvents.map((e) => ({ at: e.at, actor: 'source', type: e.type, detail: e })),
          ...rxEvents.map((e) => ({ at: e.at, actor: e.actor, type: e.type, detail: e.detail })),
        ]
          .sort((a, b) => a.at - b.at)
          .map((e) =>
            el(
              'tr',
              {},
              el('td', { class: 'num muted', text: fmtShortClock(e.at) }),
              el('td', { class: 'muted', text: e.actor }),
              el('td', { class: 'muted', text: e.type }),
              el('td', { class: 'muted wrap', text: eventSummary(e) || '—' })
            )
          )
      )
    );
    host.append(t);
  }
}

function findSessionFor(g, replays) {
  const sessions = state.ov?.backfiller?.sessions || [];
  const connIds = new Set(replays.map((f) => f.conn_id));
  const exact = sessions.find((s) => connIds.has(s.conn_id) && s.from_global <= g && s.to_global >= g);
  if (exact) return exact;
  return sessions.find((s) => connIds.has(s.conn_id)) || null;
}

function rank(f) {
  return f.origin === 'replay' ? 3 : f.origin === 'live' ? 2 : 1;
}

function stepRow(label, when, meta, body, stepState = 'done', extra = '') {
  return el(
    'div',
    { class: `step ${stepState} ${extra}` },
    el('div', { class: 'rail' }, el('div', { class: 'node' })),
    el('div', { class: 'label' }, el('div', { class: 'k', text: label }), el('div', { class: 'when', text: when }), el('div', { class: 'delta', text: meta })),
    body
  );
}
function kvPair(k, v) {
  return el('span', { class: 'pair' }, el('span', { class: 'k', text: k }), el('span', { class: 'v', text: String(v) }));
}

// ---------------------------------------------------------------- fault bench

function renderFaultBench() {
  const host = $('#fault-bench');
  if (!host || host.dataset.built) return;
  host.dataset.built = '1';
  const cards = [
    {
      title: '旧连接沉默',
      desc: '当前长连接停止跟随时钟但仍保持打开，稍后迟到补发尾部；内容与源端一致，只是来晚了。',
      btn: '让旧连接沉默',
      run: async () => post('/api/trigger', { kind: 'stall', conflictOffset: -1 }),
      note: (r) => (r.ok ? `已让 ${r.connId} 在 #${r.atGlobal} 沉默` : `失败：${r.reason || '无长连接'}`),
    },
    {
      title: '丢一串帧',
      desc: '源端在当前长连接上静默跳过 4 个号，制造只能用按序号补取才能填的洞。',
      btn: '丢 4 帧',
      run: async () => post('/api/trigger', { kind: 'drop', count: 4 }),
      note: (r) => (r.ok ? `已跳过 #${r.from}–#${r.to}` : `失败：${r.reason}`),
    },
    {
      title: '补取中重置序号',
      desc: '下一次补取会话在发出 2 帧后宣布新 epoch 并重编号；接收器必须按新 basis 解释，而不是猜。',
      btn: '安排重编号',
      run: async () => post('/api/trigger', { kind: 'renumber', after: 2 }),
      note: (r) => (r.armed ? `已安排：第 ${r.after} 帧后重编号` : '失败'),
    },
    {
      title: '两条连接内容冲突',
      desc: '旧连接迟到的尾部在某一号上与新连接的内容不同：两路内容都被封存，按来源等级裁决。',
      btn: '制造冲突',
      run: async () => {
        const hold = (state.ov?.receiver?.policy || 'overlap') === 'hold';
        if (hold) return { ok: false, reason: 'hold 策略下旧连接先退休，不会有尾部冲突' };
        return post('/api/trigger', { kind: 'stall', conflictOffset: 0, tailCount: 5 });
      },
      note: (r) =>
        r.ok
          ? `已在 ${r.connId} 的 #${r.atGlobal} 安排尾部冲突`
          : `失败：${r.reason || '无长连接'}`,
    },
    {
      title: '入库器崩溃 · 事务中',
      desc: '下一批写入事务中途进程退出：staging 批次必须整批回滚，重启后重做。',
      btn: '安排崩溃',
      run: async () => post('/api/lander', { path: '/crash', body: { mode: 'mid' } }),
      note: (r) => (r.armed ? '已安排：下一批事务中途崩溃' : '失败'),
    },
    {
      title: '入库器崩溃 · 提交后',
      desc: 'COMMIT 之后、ACK 之前退出：行已持久化但未计为进度，重启后逐行核对批次清单再确认。',
      btn: '安排崩溃',
      run: async () => post('/api/lander', { path: '/crash', body: { mode: 'after-commit' } }),
      note: (r) => (r.armed ? '已安排：提交后崩溃' : '失败'),
    },
  ];
  for (const c of cards) {
    const note = el('div', { class: 'muted', style: 'font-size:11px;min-height:15px;margin-top:6px' });
    const btn = el('button', { class: 'btn sm danger', text: c.btn });
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      note.textContent = '…';
      try {
        const r = await c.run();
        note.textContent = c.note(r);
      } catch (err) {
        note.textContent = `失败：${err.message}`;
      }
      btn.disabled = false;
      tick(true);
    });
    host.append(
      el(
        'div',
        { class: 'note', style: 'border-color:var(--line)' },
        el('h3', { text: c.title }),
        el('p', { text: c.desc }),
        btn,
        note
      )
    );
  }
  $('#fault-note').textContent =
    '这些故障都由页面通过控制 API 打到源端或入库器上：源端故障只影响它自己的编号空间，入库器故障只影响它自己的落盘进度；接收器与补取器保持运行。';
}

// ------------------------------------------------------------------- banner

function renderBanner() {
  const host = $('#banner-host');
  const ov = state.ov;
  clear(host);
  if (!ov) return;
  const rx = ov.receiver || {};
  const ld = ov.lander;
  const bf = ov.backfiller;
  if (rx.paused) {
    host.append(
      el(
        'div',
        { class: 'banner warn' },
        el('span', { text: '已暂停：接收、补取、落盘均停止前进；连接身份与游标保持不动。' }),
        el('button', { class: 'close', text: '恢复', onclick: () => togglePause(false) })
      )
    );
  }
  if (ld && ld.ok === false) {
    host.append(
      el(
        'div',
        { class: 'banner' },
        el('span', { text: `入库器不可用：${ld.error || '进程已退出'}。恢复后会先在启动时做崩溃恢复，再继续拉取。` }),
        el('button', { class: 'btn sm', text: '重启入库器', onclick: () => post('/api/lander/restart').then(() => tick(true)) })
      )
    );
  } else if (ld?.recovered?.length) {
    const r = ld.recovered[ld.recovered.length - 1];
    host.append(
      el(
        'div',
        { class: 'banner warn' },
        el('span', {
          text:
            r.kind === 'rolled-back'
              ? `入库器重启：staging 批次 ${r.batchId.slice(0, 10)}（${r.items} 项）未提交，已整批回滚。`
              : `入库器重启：批次 ${r.batchId.slice(0, 10)} 已提交未确认（${r.items} 项，缺失 ${r.missing}，冲突 ${r.divergent}），已逐行核对并补确认。`,
        }),
        el('button', { class: 'close', text: '知道了', onclick: (e) => e.target.closest('.banner').remove() })
      )
    );
  }
  if (bf && bf.ok === false) {
    host.append(el('div', { class: 'banner' }, el('span', { text: `补取器不可用：${bf.error || '进程已退出'}` })));
  }
}

// -------------------------------------------------------------------- wiring

function syncPresetButtons() {
  for (const b of document.querySelectorAll('[data-preset]')) {
    b.setAttribute('aria-pressed', String(b.dataset.preset === state.preset));
  }
}
function syncPolicyButtons() {
  for (const b of document.querySelectorAll('[data-policy]')) {
    b.setAttribute('aria-pressed', String(b.dataset.policy === state.policy));
  }
}
function syncFollowButton() {
  const b = $('#trace-follow');
  b.setAttribute('aria-pressed', String(state.follow));
  b.classList.toggle('primary', state.follow);
}

async function tick(force = false) {
  if (state.busy && !force) return;
  state.busy = true;
  try {
    const ov = await api('/api/overview');
    state.ov = ov;
    if (ov.receiver?.policy) state.policy = ov.receiver.policy;
    if (ov.run?.preset) state.preset = ov.run.preset;
    syncPresetButtons();
    syncPolicyButtons();
    state.ticks++;

    if (state.follow && ov.receiver?.cursor) {
      state.selected = ov.receiver.cursor;
      $('#trace-input').value = String(state.selected);
    }
    const needTrace =
      state.selected &&
      (state.follow || !state.trace || Date.now() - state.lastTraceAt > 2400 || state.trace.global !== state.selected);

    // Reconciliation is fetched every tick: it is the one claim that must never
    // be shown older than the landing stats beside it.
    const recPromise = api('/api/reconcile').catch(() => null);
    const tracePromise = needTrace ? loadTrace() : Promise.resolve();
    const rec = await recPromise;
    if (rec && rec.ok !== false) state.rec = rec;
    await tracePromise;
    if (!state.selected && rec && rec.ok !== false) pickDefaultTrace();

    renderRunMeta();
    renderBanner();
    renderKpis();
    renderPipeline();
    renderChecklist();
    renderMatrix();
    renderLines();
    renderConnTable();
    renderSessions();
    renderCosts();
    renderEvents();
    renderReconcile();
    renderTrace();
    renderTraceJump();
    syncPauseButton();
  } catch (err) {
    console.error(err);
  } finally {
    state.busy = false;
  }
}

function renderTraceJump() {
  const host = $('#trace-jump');
  const ov = state.ov;
  if (!ov) return;
  const rx = ov.receiver || {};
  const conflicts = rx.conflicts || [];
  const decisions = ov.ledger?.decisions || [];
  const firstReplay = decisions.find((d) => d.origin === 'replay')?.global;
  const picks = [];
  if (conflicts.length) picks.push(['冲突号', conflicts[0].global]);
  const gap = (rx.gaps || [])[0];
  if (gap) picks.push(['首个空洞', gap.global]);
  const tent = (rx.tentative || [])[0];
  if (tent) picks.push(['首个暂定', tent.global]);
  if (firstReplay) picks.push(['补取号', firstReplay]);
  if (rx.cursor) picks.push(['最新覆盖', rx.cursor]);
  clear(host);
  for (const [label, g] of picks.slice(0, 4)) {
    host.append(el('button', { class: 'btn sm ghost', text: `${label} #${g}`, onclick: () => openTrace(g) }));
  }
}

function syncPauseButton() {
  const paused = state.ov?.receiver?.paused;
  const b = $('#btn-pause');
  b.textContent = paused ? '恢复' : '暂停';
  b.classList.toggle('primary', !!paused);
  document.querySelectorAll('#btn-pause').forEach((x) => x.setAttribute('aria-pressed', String(!!paused)));
}

async function togglePause(next) {
  const paused = next != null ? next : !state.ov?.receiver?.paused;
  await post('/api/pause', { paused });
  await tick(true);
}

function wire() {
  for (const b of document.querySelectorAll('[data-preset]')) {
    b.addEventListener('click', async () => {
      state.preset = b.dataset.preset;
      syncPresetButtons();
      await post('/api/reset', { preset: state.preset, policy: state.policy });
      state.selected = null;
      state.trace = null;
      state.follow = false;
      syncFollowButton();
      await tick(true);
    });
  }
  for (const b of document.querySelectorAll('[data-policy]')) {
    b.addEventListener('click', async () => {
      state.policy = b.dataset.policy;
      syncPolicyButtons();
      await post('/api/policy', { policy: state.policy });
      await tick(true);
    });
  }
  $('#btn-reset').addEventListener('click', async () => {
    $('#btn-reset').disabled = true;
    await post('/api/reset', { preset: state.preset, policy: state.policy });
    state.selected = null;
    state.trace = null;
    $('#btn-reset').disabled = false;
    await tick(true);
  });
  $('#btn-pause').addEventListener('click', () => togglePause());
  $('#btn-settle').addEventListener('click', async () => {
    await post('/api/settle');
    await tick(true);
  });
  $('#btn-reconcile').addEventListener('click', async () => {
    const rec = await api('/api/reconcile');
    if (rec && rec.ok !== false) state.rec = rec;
    renderReconcile();
    renderChecklist();
  });
  $('#trace-go').addEventListener('click', () => openTrace(Number($('#trace-input').value)));
  $('#trace-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') openTrace(Number($('#trace-input').value));
  });
  $('#trace-follow').addEventListener('click', () => {
    state.follow = !state.follow;
    syncFollowButton();
    if (state.follow) openTraceFollow();
  });
  for (const b of document.querySelectorAll('#event-filters button')) {
    b.addEventListener('click', () => {
      state.eventFilter = b.dataset.actor;
      for (const x of document.querySelectorAll('#event-filters button')) {
        x.setAttribute('aria-pressed', String(x === b));
      }
      renderEvents();
    });
  }

  // Matrix delegation: one listener for all three bands.
  const wrap = $('#matrix-wrap');
  wrap.addEventListener('mouseover', (e) => {
    const cell = e.target.closest('.cell');
    if (!cell) return;
    showCellTip(cell, e.clientX, e.clientY);
  });
  wrap.addEventListener('mousemove', (e) => {
    if (tip.classList.contains('show')) {
      const { clientX: x, clientY: y } = e;
      positionTip(x, y);
    }
  });
  wrap.addEventListener('mouseleave', hideTip);
  wrap.addEventListener('click', (e) => {
    const cell = e.target.closest('.cell');
    if (!cell) return;
    openTrace(Number(cell.dataset.g));
  });
  wrap.addEventListener('keydown', (e) => {
    const cell = e.target.closest('.cell');
    if (!cell) return;
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      openTrace(Number(cell.dataset.g));
    }
  });

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) tick(true);
  });

  setInterval(() => {
    if (!document.hidden) tick();
  }, 1000);
  setTimeout(() => tick(true), 30);
}

function positionTip(x, y) {
  const pad = 14;
  const r = tip.getBoundingClientRect();
  let left = x + pad;
  let top = y + pad;
  if (left + r.width > window.innerWidth - 8) left = x - r.width - pad;
  if (top + r.height > window.innerHeight - 8) top = y - r.height - pad;
  tip.style.left = `${Math.max(8, left)}px`;
  tip.style.top = `${Math.max(8, top)}px`;
}

function openTraceFollow() {
  if (state.ov?.receiver?.cursor) {
    state.selected = state.ov.receiver.cursor;
    $('#trace-input').value = String(state.selected);
    loadTrace();
  }
}

/**
 * Pick something worth looking at on first paint: a conflict, else the first
 * backfilled global, else the newest covered one.
 */
function pickDefaultTrace() {
  const ov = state.ov;
  if (!ov) return;
  const conf = (ov.receiver?.conflicts || [])[0];
  const decisions = ov.ledger?.decisions || [];
  const replay = decisions.find((d) => d.origin === 'replay');
  const g = conf?.global || replay?.global || ov.receiver?.cursor;
  if (g) openTrace(g);
}

function showCellTip(cell, x, y) {
  const g = Number(cell.dataset.g);
  const s = stateOfGlobal(g);
  if (!s) return;
  const band = cell.dataset.band;
  const rows = [];
  rows.push(el('div', { class: 't-head', text: `#${g} · ${s.d?.quote?.symbol || '—'}` }));
  if (band === 'coverage') {
    const gLabel = s.grade === 'firm' ? 'firm（已确证）' : s.grade === 'tentative' ? 'tentative（尾部猜测）' : s.grade === 'gap' ? '空洞' : '尚未到达';
    rows.push(tipRow('覆盖', gLabel, true));
    if (s.d) {
      rows.push(tipRow('来源', s.d.origin));
      rows.push(tipRow('连接', `${s.d.connId} · epoch ${s.d.epoch}`));
      rows.push(tipRow('内容', `${Number(s.d.quote.px).toFixed(4)} × ${qty(s.d.quote.qty)} · ${s.d.quote.side}`));
      rows.push(tipRow('指纹', shortFp(s.d.fp)));
    }
    if (s.conf) rows.push(tipRow('冲突', `${s.conf.group_id} · ${s.conf.decision === 'pending' ? '待裁决' : '已裁决'}`));
  } else if (band === 'origin') {
    rows.push(tipRow('帧来源', s.origin || '无帧', true));
    if (s.d) {
      rows.push(tipRow('连接', `${s.d.connId} · epoch ${s.d.epoch}`));
      rows.push(tipRow('类别', s.d.origin === 'replay' ? '按序号补取' : s.d.origin === 'tail' ? '旧连接迟到尾部' : '长连接实时'));
    }
  } else {
    rows.push(tipRow('落盘', s.l ? (s.l.provisional ? '暂定行' : s.l.acked ? '已确认' : '已提交未确认') : '尚未落盘', true));
    if (s.l) {
      rows.push(tipRow('批次', String(s.l.batch_id).slice(0, 16)));
      rows.push(tipRow('修订', `r${s.l.revision}`));
      rows.push(tipRow('落盘时间', fmtShortClock(s.l.landed_at)));
    }
  }
  rows.push(el('div', { class: 't-note', text: '点击查看这一号的接收 → 补取 → 裁决 → 落盘' }));
  showTip(rows, x, y);
}

renderFaultBench();
syncFollowButton();
wire();
