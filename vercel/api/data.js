// Vercel Serverless Function — /api/data
// Supports: Service Account JWT (private sheets) or API Key (public sheets)
// Set GOOGLE_SERVICE_ACCOUNT (base64 JSON) or SHEETS_API_KEY in Vercel env vars.

const crypto         = require('crypto');
const SPREADSHEET_ID = '1Sk16vRNBUsQitL3cRUSIH86SyfQpxV9t08UW2YrSdmQ';
const RANGE          = 'Daily!A1:Q3000';

// ── Service Account JWT ───────────────────────────────────────────────
let saToken = null, saTokenExp = 0;

function b64url(buf) {
  return buf.toString('base64').replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
}

async function getServiceAccountToken(sa) {
  if (saToken && Date.now() < saTokenExp) return saToken;
  const { client_email, private_key } = sa;
  const now = Math.floor(Date.now() / 1000);
  const hdr = b64url(Buffer.from(JSON.stringify({ alg:'RS256', typ:'JWT' })));
  const pay = b64url(Buffer.from(JSON.stringify({
    iss: client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600, iat: now,
  })));
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(`${hdr}.${pay}`);
  const jwt = `${hdr}.${pay}.${b64url(sign.sign(private_key))}`;
  const resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });
  if (!resp.ok) throw new Error(`Token error: ${resp.status}`);
  const data = await resp.json();
  saToken    = data.access_token;
  saTokenExp = Date.now() + (data.expires_in - 60) * 1000;
  return saToken;
}

// ── Data processing (same logic as server.js) ────────────────────────

function normalizeStr(s) {
  if (!s || s.trim() === '' || s === '.0') return null;
  const str = s.trim();
  if (str.includes('/')) {
    const [datePart, timePart = '00:00:00'] = str.split(' ');
    const [m, d, y] = datePart.split('/');
    const [hh, mm, ss = '00'] = timePart.split(':');
    return `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}T${hh.padStart(2,'0')}:${mm}:${ss}`;
  }
  const [datePart, timePart = '00:00:00'] = str.split(' ');
  const [hh, mm, ss = '00'] = timePart.split(':');
  return `${datePart}T${hh.padStart(2,'0')}:${mm}:${ss}`;
}

function extractTime(s) {
  const n = normalizeStr(s);
  return n ? n.substring(11, 16) : '';
}

function perdeuCPT(row) {
  const robo = normalizeStr(row[9]);
  const plan = normalizeStr(row[4]);
  if (!robo || !plan) return false;
  return robo > plan;
}

function parseShipments(s) {
  if (!s || s === '.0' || s === '0.0' || s === '0') return 0;
  return Math.round(parseFloat(s.trim().replace(/\./g, '').replace(',', '.')) || 0);
}

// Pacotes_Real (col P, index 15): use if filled; fallback to Shipments (col M, index 12)
function getShipments(r) {
  const real = r[15];
  if (real && real !== '.0' && real !== '0' && real !== '0.0') return parseShipments(real);
  return parseShipments(r[12]);
}

const CARREGADAS = new Set(['Carregado', 'Carregado/Liberado', 'Finalizado']);

function processRawData(raw) {
  const rows    = Array.isArray(raw.values) ? raw.values.slice(1) : [];
  const byDate  = {};
  const allRows = [];

  rows.forEach((r, i) => {
    // Date_SoC (col H, index 7) = operational date; fallback to date_cpt (col A)
    const dateSoc = (r[7] || r[0] || '').substring(0, 10);
    if (!dateSoc || dateSoc.length < 10) return;

    const turno   = r[13] || '';
    if (!turno) return;

    const destino = r[11] || '';
    const doca    = r[14] || '';
    const statusR = r[10] || '';
    const pct     = perdeuCPT(r);
    const ship    = getShipments(r);  // Pacotes_Real (col P) se preenchido, senão Shipments (col M)
    const isCarr  = CARREGADAS.has(statusR);

    allRows.push({
      d: dateSoc, lt: r[1]||'', vt: r[2]||'',
      ep: extractTime(r[3]), cp: extractTime(r[4]), cr: extractTime(r[9]),
      sr: statusR, dest: destino, doca, tr: turno, ship, pct: pct ? 1 : 0,
      rowNum: i + 2,  // linha real na planilha (header=1, dados a partir de 2)
      just: r[16] || '',  // Col Q — justificativa da perda de CPT
    });

    if (!byDate[dateSoc]) byDate[dateSoc] = {};
    if (!byDate[dateSoc][turno]) byDate[dateSoc][turno] = {
      total:0, statusReal:{}, destinos:{}, docas:{}, perdeuCPT:0,
      totalShip:0, carregadas:0, shipCarregadas:0
    };
    const tg = byDate[dateSoc][turno];
    tg.total++;
    tg.totalShip += ship;
    tg.statusReal[statusR] = (tg.statusReal[statusR]||0) + 1;
    if (destino) tg.destinos[destino] = (tg.destinos[destino]||0) + 1;
    if (doca)    tg.docas[doca]       = (tg.docas[doca]||0) + 1;
    if (pct)     tg.perdeuCPT++;
    if (isCarr)  { tg.carregadas++; tg.shipCarregadas += ship; }
  });

  const dates = Object.keys(byDate).sort();
  return { DATES: dates, BY_DATE: byDate, ALL_ROWS: allRows,
           generatedAt: Date.now(), rowCount: allRows.length };
}

// ── Handler ──────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate=120');

  try {
    let sheetsUrl = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(RANGE)}`;
    let headers   = {};

    if (process.env.GOOGLE_SERVICE_ACCOUNT) {
      // ── Service Account JWT (private/domain-restricted sheets) ──────
      const sa    = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT, 'base64').toString('utf8'));
      const token = await getServiceAccountToken(sa);
      headers     = { Authorization: `Bearer ${token}` };
    } else if (process.env.SHEETS_API_KEY) {
      // ── API Key (public sheets only) ─────────────────────────────────
      sheetsUrl += `?key=${process.env.SHEETS_API_KEY}`;
    } else {
      return res.status(500).json({ error: 'Configure GOOGLE_SERVICE_ACCOUNT ou SHEETS_API_KEY nas variáveis de ambiente do Vercel.' });
    }

    const response = await fetch(sheetsUrl, { headers });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Sheets API ${response.status}: ${body.substring(0, 300)}`);
    }
    const raw  = await response.json();
    const data = processRawData(raw);
    res.json(data);
  } catch (err) {
    console.error('[api/data] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
};
