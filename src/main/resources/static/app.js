/* 서버 모드는 읽기 전용. polling과 realtime은 같은 화면과 API 모델을 공유한다. */
const POLLING_INTERVAL_MS = 2000;
const REQUEST_TIMEOUT_MS = 10000;
const ui = Object.fromEntries(['connection', 'mode-label', 'retry', 'choices', 'candidates',
  'candidate-state', 'candidate-count', 'user-id', 'vote-form', 'vote-button', 'vote-button-text',
  'vote-message', 'rankings', 'ranking-state', 'total-votes', 'updated-at', 'update-help',
  'candidate-form', 'candidate-name', 'candidate-add-button', 'candidate-message',
  'candidate-toggle', 'candidate-add-panel']
  .map(id => [id, document.getElementById(id)]));
let candidates = [];
let snapshot = null;
let mode;
let sending = false;
let source;
let pollingTimer;
let stopped = false;
let refreshingCandidates = false;
let addingCandidate = false;
const number = new Intl.NumberFormat('ko-KR');

function connection(text, state) {
  ui.connection.textContent = text;
  ui.connection.dataset.state = state;
}

async function request(path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(path, { ...options, cache: 'no-store', signal: controller.signal });
    const body = await response.json();
    if (!response.ok) {
      const error = new Error(body.message || '요청을 처리하지 못했어요.');
      error.status = response.status;
      error.code = body.code;
      throw error;
    }
    return body;
  } finally {
    clearTimeout(timer);
  }
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function updateButton() {
  const selected = ui.choices.querySelector('input:checked');
  ui['vote-button'].disabled = sending || !selected || !ui['user-id'].value.trim();
  ui['vote-button-text'].textContent = sending ? '투표를 보내고 있어요…'
    : !selected ? '메뉴를 선택해 주세요' : !ui['user-id'].value.trim() ? '사용자 ID를 입력해 주세요' : '이 메뉴에 투표하기';
}

function renderCandidates() {
  const selected = ui.choices.querySelector('input:checked')?.value;
  const fragment = document.createDocumentFragment();
  for (const candidate of candidates) {
    const label = element('label', 'candidate');
    const input = document.createElement('input');
    input.type = 'radio';
    input.name = 'candidateId';
    input.value = String(candidate.id);
    input.required = true;
    input.checked = input.value === selected;
    label.append(input, element('span', 'candidate-name', candidate.name));
    fragment.append(label);
  }
  ui.candidates.replaceChildren(fragment);
  ui.choices.disabled = sending || candidates.length === 0;
  ui['candidate-count'].textContent = `후보 ${number.format(candidates.length)}개`;
  ui['candidate-state'].textContent = candidates.length ? '' : '아직 등록된 후보가 없어요. 후보 추가 버튼으로 첫 메뉴를 추가해 주세요.';
  ui['candidate-state'].hidden = candidates.length > 0;
  updateButton();
}

function renderRanking() {
  if (snapshot === null) return;
  const byId = new Map(candidates.map(candidate => [candidate.id, candidate]));
  const rankedIds = new Set(snapshot.map(row => row.candidateId));
  // Redis가 정한 순서 그대로 사용한다. 미집계 후보만 목록 뒤에 0표로 추가한다.
  const rows = snapshot.map(row => ({ ...row, name: byId.get(row.candidateId)?.name ?? `후보 #${row.candidateId}` }));
  rows.push(...candidates.filter(candidate => !rankedIds.has(candidate.id))
    .map(candidate => ({ candidateId: candidate.id, name: candidate.name, voteCount: 0 })));
  const total = rows.reduce((sum, row) => sum + row.voteCount, 0);
  const fragment = document.createDocumentFragment();
  rows.forEach((row, index) => {
    const percentage = total === 0 ? 0 : row.voteCount / total * 100;
    const item = element('li', 'ranking-row');
    const detail = element('div');
    const heading = element('div', 'rank-heading');
    heading.append(element('span', 'rank-name', row.name),
      element('span', 'rank-count', `${number.format(row.voteCount)}표 · ${percentage.toFixed(1)}%`));
    const track = element('div', 'rank-track');
    track.setAttribute('aria-hidden', 'true');
    const fill = element('div', 'rank-fill');
    fill.style.width = `${percentage}%`;
    track.append(fill);
    detail.append(heading, track);
    item.append(element('span', 'rank-number', String(index + 1)), detail);
    fragment.append(item);
  });
  ui.rankings.replaceChildren(fragment);
  ui['total-votes'].textContent = number.format(total);
  ui['ranking-state'].textContent = rows.length === 0 ? '후보가 등록되면 랭킹을 볼 수 있어요.'
    : total === 0 ? '아직 집계된 투표가 없어요. 첫 번째 선택을 남겨 보세요.' : '';
  ui['ranking-state'].hidden = rows.length > 0 && total > 0;
}

function applySnapshot(data) {
  if (!Array.isArray(data) || data.some(row => !Number.isSafeInteger(row.candidateId)
    || !Number.isSafeInteger(row.voteCount) || row.voteCount < 0)) throw new Error('잘못된 랭킹 응답');
  snapshot = data; // ranking 이벤트는 증가분이 아닌 전체 스냅샷이다.
  renderRanking();
  const now = new Date();
  ui['updated-at'].dateTime = now.toISOString();
  ui['updated-at'].textContent = now.toLocaleTimeString('ko-KR', { hour12: false });
  if (snapshot.some(row => !candidates.some(candidate => candidate.id === row.candidateId))) refreshCandidates();
}

async function refreshCandidates() {
  if (refreshingCandidates || stopped) return;
  refreshingCandidates = true;
  try {
    mergeCandidates(await request('/api/candidates'));
    renderCandidates();
    renderRanking();
  } catch {
    ui['candidate-state'].hidden = false;
    ui['candidate-state'].textContent = '새 후보 이름을 불러오지 못했어요. 화면을 다시 불러와 주세요.';
    ui.retry.hidden = false;
  } finally {
    refreshingCandidates = false;
  }
}

function mergeCandidates(incoming) {
  // 후보는 현재 추가만 가능하다. 늦게 도착한 조회 응답이 방금 등록한 후보를 지우지 않게 합친다.
  const byId = new Map(candidates.map(candidate => [candidate.id, candidate]));
  incoming.forEach(candidate => byId.set(candidate.id, candidate));
  candidates = [...byId.values()].sort((first, second) => first.id - second.id);
}

function toggleCandidatePanel(open) {
  ui['candidate-add-panel'].hidden = !open;
  ui['candidate-toggle'].setAttribute('aria-expanded', String(open));
  ui['candidate-toggle'].textContent = open ? '닫기' : '+ 후보 추가';
  if (open && !ui['candidate-name'].disabled) ui['candidate-name'].focus();
  if (!open) ui['candidate-toggle'].focus();
}

ui['candidate-toggle'].addEventListener('click', () => toggleCandidatePanel(ui['candidate-add-panel'].hidden));
ui['candidate-add-panel'].addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    event.preventDefault();
    toggleCandidatePanel(false);
  }
});

ui['candidate-form'].addEventListener('submit', async event => {
  event.preventDefault();
  if (addingCandidate || ui['candidate-name'].disabled || !ui['candidate-form'].reportValidity()) return;
  const name = ui['candidate-name'].value.trim();
  const message = ui['candidate-message'];
  if (!name) {
    message.dataset.state = 'error';
    message.textContent = '공백만 입력할 수 없어요. 메뉴 이름을 입력해 주세요.';
    ui['candidate-name'].focus();
    return;
  }
  addingCandidate = true;
  ui['candidate-name'].disabled = true;
  ui['candidate-add-button'].disabled = true;
  ui['candidate-add-button'].textContent = '추가 중…';
  ui['candidate-form'].setAttribute('aria-busy', 'true');
  message.dataset.state = 'sending';
  message.textContent = '후보를 추가하고 있어요.';
  try {
    const candidate = await request('/api/candidates', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name })
    });
    // 저장된 ID와 정규화된 이름만 반영. 기존 선택과 득표 스냅샷은 유지한다.
    mergeCandidates([candidate]);
    renderCandidates();
    renderRanking();
    ui['candidate-name'].value = '';
    message.dataset.state = 'success';
    message.textContent = `“${candidate.name}” 후보를 추가했어요. 위 목록에서 선택해 투표해 주세요.`;
  } catch (error) {
    message.dataset.state = 'error';
    message.textContent = error.code === 'DUPLICATE_CANDIDATE'
      ? '이미 등록된 메뉴 이름이에요. 다른 이름을 입력해 주세요.'
      : error.status ? `후보를 추가하지 못했어요. ${error.message}`
        : '응답을 확인하지 못했어요. 저장됐을 수도 있으니 화면을 새로고침해 목록을 확인해 주세요.';
  } finally {
    addingCandidate = false;
    ui['candidate-name'].disabled = false;
    ui['candidate-add-button'].disabled = false;
    ui['candidate-add-button'].textContent = '후보 추가';
    ui['candidate-form'].setAttribute('aria-busy', 'false');
    if (!ui['candidate-add-panel'].hidden) ui['candidate-name'].focus();
  }
});

function rankingError() {
  ui['ranking-state'].hidden = false;
  ui['ranking-state'].textContent = snapshot === null ? '랭킹을 불러오지 못했어요. 연결을 다시 시도하고 있어요.'
    : '연결이 원활하지 않아 마지막으로 받은 결과를 표시하고 있어요.';
  ui.retry.hidden = false;
}

async function poll() {
  try {
    applySnapshot(await request('/api/votes/results'));
    connection('연결됨 · 주기적으로 확인하고 있어요', 'connected');
    ui.retry.hidden = true;
  } catch {
    connection('연결 오류 · 자동으로 다시 시도해요', 'error');
    rankingError();
  } finally {
    // 완료 후 예약하므로 느린 응답에서도 요청이 겹치지 않는다.
    if (!stopped) pollingTimer = setTimeout(poll, POLLING_INTERVAL_MS);
  }
}

function subscribe() {
  connection('연결 중 · 실시간 랭킹을 연결하고 있어요', 'connecting');
  source = new EventSource('/api/votes/rankings/stream');
  source.onopen = () => connection('연결됨 · 실시간으로 받고 있어요', 'connected');
  source.addEventListener('ranking', event => {
    try {
      applySnapshot(JSON.parse(event.data));
      connection('연결됨 · 실시간으로 받고 있어요', 'connected');
      ui.retry.hidden = true;
    } catch {
      connection('연결 오류 · 랭킹 응답을 확인하지 못했어요', 'error');
      rankingError();
    }
  });
  source.onerror = () => {
    connection('재연결 중 · 실시간 연결을 다시 시도해요', 'error');
    rankingError();
    // EventSource의 자동 재연결을 사용한다. 별도의 반복 연결을 만들지 않는다.
  };
}

ui['vote-form'].addEventListener('submit', async event => {
  event.preventDefault();
  if (sending || !ui['vote-form'].reportValidity()) return;
  const selected = ui.choices.querySelector('input:checked');
  const userId = ui['user-id'].value.trim();
  if (!selected || !userId) return;
  sending = true;
  ui.choices.disabled = true;
  ui['user-id'].disabled = true;
  ui['vote-form'].setAttribute('aria-busy', 'true');
  updateButton();
  ui['vote-message'].dataset.state = 'sending';
  ui['vote-message'].textContent = '투표를 보내고 있어요. 잠시만 기다려 주세요.';
  try {
    await request('/api/votes', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, candidateId: Number(selected.value) }) });
    ui['vote-message'].dataset.state = 'success';
    ui['vote-message'].textContent = '투표가 접수됐어요! 랭킹에는 서버의 집계 결과가 반영됩니다.';
    // 성공 응답만으로 표 수를 변경하지 않는다. polling/SSE 결과를 기다린다.
  } catch (error) {
    ui['vote-message'].dataset.state = 'error';
    ui['vote-message'].textContent = error.code === 'ALREADY_VOTED'
      ? '이미 투표한 사용자 ID예요. 한 ID당 한 번만 투표할 수 있어요.'
      : error.status ? `투표하지 못했어요. ${error.message}`
        : '응답을 확인하지 못했어요. 투표가 저장됐을 수도 있으니 같은 ID로 다시 확인해 주세요.';
  } finally {
    sending = false;
    ui.choices.disabled = candidates.length === 0;
    ui['user-id'].disabled = false;
    ui['vote-form'].setAttribute('aria-busy', 'false');
    updateButton();
  }
});
ui.choices.addEventListener('change', updateButton);
ui['user-id'].addEventListener('input', updateButton);
ui.retry.addEventListener('click', () => location.reload());
window.addEventListener('pagehide', () => {
  stopped = true;
  clearTimeout(pollingTimer);
  source?.close();
});
window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });

async function initialize() {
  try {
    const config = await request('/api/mode');
    if (!['polling', 'realtime'].includes(config.mode)) throw new Error('알 수 없는 실행 모드');
    mode = config.mode;
    ui['mode-label'].textContent = mode === 'polling' ? '주기 조회 · 2초' : '실시간 업데이트';
    ui['update-help'].textContent = mode === 'polling' ? 'DB 집계 결과를 조회 완료 후 2초마다 확인해요.'
      : '집계가 완료되면 자동으로 갱신돼요. 연결이 끊기면 최신 결과로 다시 이어집니다.';
    candidates = await request('/api/candidates');
    renderCandidates();
    ui['candidate-name'].disabled = false;
    ui['candidate-add-button'].disabled = false;
    if (stopped) return;
    if (mode === 'polling') poll(); else subscribe();
  } catch {
    connection('연결 오류 · 서버에 연결하지 못했어요', 'error');
    ui['candidate-state'].textContent = '화면을 준비하지 못했어요. 잠시 후 다시 불러와 주세요.';
    ui['ranking-state'].textContent = '서버 연결 후 랭킹을 확인할 수 있어요.';
    ui.retry.hidden = false;
  }
}
initialize();
