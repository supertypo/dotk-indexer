const base = document.querySelector('main').dataset.base;
const POLL_MS = 2000;
const FETCH_MS = 5000;
const UNITS = [[86400,'d'],[3600,'h'],[60,'m'],[1,'s']];
function duration(secs) {
  const [div, unit] = UNITS.find(([d]) => secs >= d) ?? UNITS[UNITS.length - 1];
  return Math.floor(secs / div) + unit;
}
/** Lag behind the sink, less the confirmation depth the indexer keeps. */
function behind(h) {
  if (h.tipBlueScore == null || h.lastBlock == null) return '–';
  return duration(Math.max(0, h.tipBlueScore - h.tipDistance - h.lastBlock.blueScore) / h.netBps);
}
const el = (id) => document.getElementById(id);
const set = (id, v) => el(id).textContent = v;

const ICON_COPY = '<svg viewBox="0 0 24 24"><path d="M19,21H8V7H19M19,5H8A2,2 0 0,0 6,7V21A2,2 0 0,0 8,23H19A2,2 0 0,0 21,21V7A2,2 0 0,0 19,5M16,1H4A2,2 0 0,0 2,3V17H4V3H16V1Z"/></svg>';
const ICON_DONE = '<svg viewBox="0 0 24 24"><path d="M21,7L9,19L3.5,13.5L4.91,12.09L9,16.17L19.59,5.59L21,7Z"/></svg>';

for (const btn of document.querySelectorAll('.copy')) {
  btn.innerHTML = ICON_COPY;
  btn.addEventListener('click', async () => {
    const text = el(btn.dataset.copies).title;
    if (!text) return;
    // The clipboard API needs a secure context, so plain-http access uses the selection fallback.
    try {
      if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
      else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.cssText = 'position:fixed;opacity:0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        ta.remove();
      }
    } catch {
      return;
    }
    btn.innerHTML = ICON_DONE;
    btn.title = 'copied';
    setTimeout(() => { btn.innerHTML = ICON_COPY; btn.title = 'copy' }, 2000);
  });
}

function identifier(id, value) {
  el(id).textContent = value ?? '–';
  el(id).title = value ?? '';
}

async function refresh() {
  const pill = el('status-pill');
  try {
    const res = await fetch(base + '/health', { signal: AbortSignal.timeout(FETCH_MS) });
    const h = await res.json();
    if (typeof h.healthy !== 'boolean') throw new Error('not a health body');
    setPill(pill, h.healthy ? 'healthy' : 'unhealthy', h.healthy ? 'ok' : 'bad');
    set('network-pill', h.network ?? '');

    set('names', h.active);
    set('pending', h.pending);
    set('owner-unknown', h.ownerUnknown);
    set('historySeq', h.historySeq ?? '–');
    identifier('covenant-id', h.registryCovenantId);

    set('behind', behind(h));
    set('tip', h.tipDistance);
    set('journal', h.journalCoverage == null ? 'none' : h.journalCoverage === 0 ? 'full' : h.journalCoverage);
    const b = h.lastBlock;
    identifier('hash', b?.hash);
    el('block-meta').innerHTML = b == null ? ''
      : [['daa', b.daaScore], ['blue', b.blueScore], ['time', new Date(b.timestamp).toISOString()]]
          .map(([k, v]) => `<span><i>${k}</i>${v}</span>`).join('');

    const t = h.selfTest;
    const failed = t != null && !t.proven;
    set('selftest', t == null ? (h.caughtUp ? 'running…' : 'catching up') : (failed ? 'failed' : 'proven'));
    el('selftest').className = 'v' + (failed ? ' bad' : '');
    // The age uses the browser clock.
    const proof = el('proof');
    proof.textContent = t == null ? '–' : duration(Math.max(0, Date.now() - t.finishedMs) / 1000) + ' ago';
    proof.title = t == null ? '' : new Date(t.finishedMs).toISOString();
    const ms = t == null ? 0 : t.finishedMs - t.startedMs;
    set('proof-took', t == null ? '' : 'took ' + (ms < 1000 ? ms + ' ms' : (ms / 1000).toFixed(1) + ' s'));
    set('checked', t == null ? '–' : t.gapsChecked + ' / ' + t.deedsChecked);
    el('selftest-row').hidden = !failed;
    // Fetch the detail only on failure, so the polled body stays small.
    if (failed) {
      const full = await (await fetch(base + '/health?detail=true', { signal: AbortSignal.timeout(FETCH_MS) })).json();
      set('selftest-detail', JSON.stringify({ ...full.selfTest, ...full.selfTestDetail }, null, 2));
    }
  } catch {
    setPill(pill, 'unreachable', 'bad');
  }
}
// Written only on change, so the live region is not announced on every poll.
function setPill(pill, text, cls) {
  if (pill.textContent !== text) pill.textContent = text;
  pill.className = 'pill ' + cls;
}
// One poll in flight at a time, and none while the tab is hidden.
let timer;
let running = false;
async function poll() {
  clearTimeout(timer);
  if (running) return;
  running = true;
  try {
    if (!document.hidden) await refresh();
  } finally {
    running = false;
  }
  timer = setTimeout(poll, POLL_MS);
}
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) poll();
});
poll();
