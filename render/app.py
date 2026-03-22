from flask import Flask, jsonify, request
from flask_cors import CORS
import os, json, time
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)
CORS(app)

SHEET_ID   = '1vHTAhCR0VRRFCt1qoI_hd1IzUYxmd2-76--OIF1ojkk'
SHEET_NAME = 'Base'
RANGE      = 'Base!A1:Q3000'

_cache     = {'data': None, 'ts': 0}
CACHE_TTL  = 60

# Índices das colunas (aba Base):
# 0:date_cpt  1:LT(Nº Viagem)  2:vehicle_type  3:eta_plan  4:cpt_plan  5:cpt_realized
# 6:Status_trip  7:Date_SoC  8:Turno_cpt_plan  9:cpt_real_robô  10:Status_Real
# 11:Destino  12:Shipments  13:Turno_Real  14:Doca  15:Pacotes_Real  16:justificativa(Q)

CARREGADAS = {'Carregado', 'Carregado/Liberado', 'Finalizado'}


# ── Credenciais ──────────────────────────────────────────────────────────

def get_credentials():
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if creds_json:
        return Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    return Credentials.from_service_account_file('credencial.json', scopes=scope)


# ── Helpers ────────────────────────────────────────────────────────────────

def normalize_str(s):
    if not s or str(s).strip() in ('', '.0'):
        return None
    s = str(s).strip()
    if '/' in s:
        parts = s.split(' ')
        date_part = parts[0]
        time_part = parts[1] if len(parts) > 1 else '00:00:00'
        m, d, y = date_part.split('/')
        hh, mm, *rest = time_part.split(':')
        ss = rest[0] if rest else '00'
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}T{hh.zfill(2)}:{mm}:{ss}"
    parts = s.split(' ')
    date_part = parts[0]
    time_part = parts[1] if len(parts) > 1 else '00:00:00'
    hh, mm, *rest = time_part.split(':')
    ss = rest[0] if rest else '00'
    return f"{date_part}T{hh.zfill(2)}:{mm}:{ss}"


def extract_time(s):
    n = normalize_str(s)
    return n[11:16] if n else ''


def perdeu_cpt(row):
    robo = normalize_str(row[9] if len(row) > 9 else '')
    plan = normalize_str(row[4] if len(row) > 4 else '')
    if not robo or not plan:
        return False
    return robo > plan


def parse_shipments(s):
    if not s or str(s).strip() in ('.0', '0.0', '0', ''):
        return 0
    cleaned = str(s).strip().replace('.', '').replace(',', '.')
    try:
        return round(float(cleaned))
    except ValueError:
        return 0


def get_shipments(row):
    real = row[15] if len(row) > 15 else ''
    if real and str(real).strip() not in ('.0', '0', '0.0', ''):
        return parse_shipments(real)
    return parse_shipments(row[12] if len(row) > 12 else '')


# ── Processamento ──────────────────────────────────────────────────────────

def process_raw_data(all_values):
    rows    = all_values[1:]
    by_date = {}
    all_rows = []

    for i, r in enumerate(rows):
        while len(r) < 17:
            r.append('')

        date_soc = (r[7] or r[0] or '')[:10]
        if not date_soc or len(date_soc) < 10:
            continue

        turno = r[13] or ''
        if not turno:
            continue

        destino  = r[11] or ''
        doca     = r[14] or ''
        status_r = r[10] or ''
        pct      = perdeu_cpt(r)
        ship     = get_shipments(r)
        is_carr  = status_r in CARREGADAS

        all_rows.append({
            'd':      date_soc,
            'lt':     r[1]  or '',
            'vt':     r[2]  or '',
            'ep':     extract_time(r[3]),
            'cp':     extract_time(r[4]),
            'cr':     extract_time(r[9]),
            'sr':     status_r,
            'dest':   destino,
            'doca':   doca,
            'tr':     turno,
            'ship':   ship,
            'pct':    1 if pct else 0,
            'rowNum': i + 2,
            'just':   r[16] or '',
        })

        if date_soc not in by_date:
            by_date[date_soc] = {}
        if turno not in by_date[date_soc]:
            by_date[date_soc][turno] = {
                'total': 0, 'statusReal': {}, 'destinos': {}, 'docas': {},
                'perdeuCPT': 0, 'totalShip': 0, 'carregadas': 0, 'shipCarregadas': 0,
            }

        tg = by_date[date_soc][turno]
        tg['total']    += 1
        tg['totalShip'] += ship
        tg['statusReal'][status_r]  = tg['statusReal'].get(status_r, 0) + 1
        if destino:
            tg['destinos'][destino] = tg['destinos'].get(destino, 0) + 1
        if doca:
            tg['docas'][doca]       = tg['docas'].get(doca, 0) + 1
        if pct:
            tg['perdeuCPT'] += 1
        if is_carr:
            tg['carregadas']     += 1
            tg['shipCarregadas'] += ship

    dates = sorted(by_date.keys())
    return {
        'DATES':       dates,
        'BY_DATE':     by_date,
        'ALL_ROWS':    all_rows,
        'generatedAt': int(time.time() * 1000),
        'rowCount':    len(all_rows),
    }


# ── Rotas ──────────────────────────────────────────────────────────────────

@app.route('/api/dados')
def dados():
    now = time.time()
    if _cache['data'] is not None and now - _cache['ts'] < CACHE_TTL:
        return jsonify(_cache['data'])
    try:
        client     = gspread.authorize(get_credentials())
        sheet      = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        all_values = sheet.get_all_values()
        result     = process_raw_data(all_values)
        _cache.update({'data': result, 'ts': now})
        return jsonify(result)
    except Exception as e:
        print('[api/dados] Erro:', str(e))
        return jsonify({'error': str(e)}), 500


@app.route('/api/salvar-justificativa', methods=['POST', 'OPTIONS'])
def salvar():
    if request.method == 'OPTIONS':
        return '', 200

    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'Payload vazio'}), 400

    row_num = data.get('rowNum')
    text    = data.get('text', '')

    if not row_num or int(row_num) < 2:
        return jsonify({'success': False, 'error': 'rowNum inválido'}), 400

    try:
        client = gspread.authorize(get_credentials())
        ws     = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        ws.update(f'Q{int(row_num)}', [[text]])
        print(f'[justify] Linha {row_num} atualizada: "{text}"')
        return jsonify({'ok': True})
    except Exception as e:
        print('[justify] Erro:', str(e))
        return jsonify({'success': False, 'error': str(e)}), 500
