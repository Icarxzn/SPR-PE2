// Vercel Serverless Function — POST /api/justify
// Salva justificativa de perda de CPT na coluna Q da planilha

const crypto         = require('crypto');
const SPREADSHEET_ID = '1Sk16vRNBUsQitL3cRUSIH86SyfQpxV9t08UW2YrSdmQ';

function b64url(buf) {
  return buf.toString('base64').replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
}

async function getServiceAccountToken(sa) {
  const { client_email, private_key } = sa;
  const now = Math.floor(Date.now() / 1000);
  const hdr = b64url(Buffer.from(JSON.stringify({ alg:'RS256', typ:'JWT' })));
  const pay = b64url(Buffer.from(JSON.stringify({
    iss: client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',  // read+write
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
  return data.access_token;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'Method not allowed' }); return;
  }

  try {
    const { rowNum, text } = req.body;
    if (!rowNum || rowNum < 2) throw new Error('rowNum inválido');

    if (!process.env.GOOGLE_SERVICE_ACCOUNT) {
      res.status(501).json({ error: 'GOOGLE_SERVICE_ACCOUNT não configurado no Vercel' });
      return;
    }

    const sa    = JSON.parse(Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT, 'base64').toString('utf8'));
    const token = await getServiceAccountToken(sa);
    const range = `Daily!Q${rowNum}`;
    const url   = `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`;

    const sheetsResp = await fetch(url, {
      method: 'PUT',
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ values: [[text]] }),
    });

    if (!sheetsResp.ok) {
      const errText = await sheetsResp.text();
      throw new Error(`Sheets write ${sheetsResp.status}: ${errText}`);
    }

    console.log(`[justify] Linha ${rowNum} atualizada: "${text}"`);
    res.status(200).json({ ok: true });
  } catch (e) {
    console.error('[justify] Erro:', e.message);
    res.status(500).json({ error: e.message });
  }
};
