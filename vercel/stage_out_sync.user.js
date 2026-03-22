// ==UserScript==
// @name         SPX Stage Out → Dashboard Sync
// @namespace    http://tampermonkey.net/
// @version      1.1
// @description  Sincroniza posições do staging outbound com o dashboard local
// @match        https://spx.shopee.com.br/*
// @grant        GM_xmlhttpRequest
// @connect      localhost
// ==/UserScript==

(function () {
  'use strict';

  const SERVER_URL = 'http://localhost:4567/api/stage-data';
  const SEARCH_URL = '/api/in-station/outbound/outbound_staging_area/config/search';
  const PAGE_SIZE  = 100;
  const INTERVAL   = 60 * 1000; // 60s

  function getCsrf() {
    const m = document.cookie.match(/csrftoken=([^;]+)/);
    return m ? m[1] : '';
  }

  async function fetchPage(pageno) {
    const res = await fetch(SEARCH_URL, {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        'x-csrftoken': getCsrf(),
      },
      body: JSON.stringify({ pageno, count: PAGE_SIZE }),
    });
    return res.json();
  }

  async function fetchAll() {
    const first = await fetchPage(1);
    if (first.retcode !== 0) throw new Error('API retcode: ' + first.retcode + ' — ' + first.message);
    const { total, list } = first.data;
    const pages = Math.ceil(total / PAGE_SIZE);
    let all = [...list];
    for (let p = 2; p <= pages; p++) {
      const r = await fetchPage(p);
      if (r.retcode === 0) all = all.concat(r.data.list);
    }
    return { list: all, total, fetchedAt: Date.now() };
  }

  function sendToServer(data) {
    GM_xmlhttpRequest({
      method: 'POST',
      url: SERVER_URL,
      headers: { 'Content-Type': 'application/json' },
      data: JSON.stringify(data),
      onload: (r) => {
        dot.textContent = '✅ Sync ' + new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
        dot.style.background = '#2db55d';
        console.log('[SPX Sync] Enviado:', data.list.length, '/', data.total, 'posições');
      },
      onerror: (e) => {
        dot.textContent = '❌ Server offline';
        dot.style.background = '#cc0000';
        console.error('[SPX Sync] Erro ao enviar para server local:', e);
      },
    });
  }

  async function sync() {
    dot.textContent = '🔄 Sincronizando...';
    dot.style.background = '#ee4d2d';
    try {
      const data = await fetchAll();
      sendToServer(data);
    } catch (e) {
      dot.textContent = '⚠️ Erro API SPX';
      dot.style.background = '#cc7700';
      console.error('[SPX Sync]', e);
    }
  }

  // ── Visual indicator ────────────────────────────────────────────────
  const dot = document.createElement('div');
  dot.style.cssText = [
    'position:fixed', 'bottom:16px', 'right:16px',
    'background:#ee4d2d', 'color:#fff',
    'padding:6px 14px', 'border-radius:20px',
    'font-size:12px', 'font-family:sans-serif',
    'z-index:2147483647', 'cursor:pointer',
    'box-shadow:0 2px 8px rgba(0,0,0,0.4)',
    'user-select:none',
  ].join(';');
  dot.textContent = '📡 Dashboard Sync';
  dot.title = 'Clique para sincronizar agora';
  dot.addEventListener('click', sync);
  document.body.appendChild(dot);

  // ── Run ─────────────────────────────────────────────────────────────
  sync();
  setInterval(sync, INTERVAL);
})();
