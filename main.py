# main.py
# -*- coding: utf-8 -*-
import os
import json
import csv
import time
import math
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv

# Carrega as variáveis de ambiente dos ficheiros .env
load_dotenv()
if os.path.exists('.env.local'):
    load_dotenv(dotenv_path='.env.local', override=True)
    print("-> A executar em modo LOCAL.")
else:
    print("-> A executar em modo PRODUÇÃO (GitHub Actions).")

# =========================
# CONFIG
# =========================
BASE_URL = "https://digitalmanager.guru/api/v2"
DMG_USER_TOKEN = os.getenv("DMG_USER_TOKEN")
if not DMG_USER_TOKEN:
    raise ValueError("O token DMG_USER_TOKEN não foi encontrado. Verifique o seu .env ou os Secrets do GitHub.")

END_DATE   = os.getenv("END_DATE", datetime.now().strftime("%Y-%m-%d"))
OUT_DIR = "./out"
os.makedirs(OUT_DIR, exist_ok=True)
DMG_TZ = "America/Sao_Paulo"
PAGE_SIZE = 200
REQUEST_TIMEOUT = (25, 120)
API_MAX_RANGE_DAYS = 180
MIN_DATE_ALL = datetime.now().strftime("%Y-01-01")
SUBS_CREATED_AT_INI = MIN_DATE_ALL

# =========================
# FUNÇÕES AUXILIARES DE DATA E HORA
# =========================
def _tz(): return pytz.timezone(DMG_TZ)

def to_tz(dt_str, tzname=DMG_TZ):
    if not dt_str: return None
    tz = pytz.timezone(tzname)
    fmt = "%Y-%m-%d %H:%M:%S" if " " in dt_str else "%Y-%m-%d"
    dt_naive = datetime.strptime(dt_str[:19], fmt) if " " in dt_str else datetime.strptime(dt_str[:10], fmt)
    return tz.localize(dt_naive)

def from_iso_any(s):
    if s in (None, "", 0): return None
    tz = _tz()
    if isinstance(s, (int, float)):
        ts = float(s); ts = ts/1000.0 if ts > 1e12 else ts
        return datetime.fromtimestamp(ts, tz)
    if isinstance(s, str):
        s2 = s.strip()
        if not s2: return None
        if s2.replace(".", "", 1).isdigit():
            ts = float(s2); ts = ts/1000.0 if ts > 1e12 else ts
            return datetime.fromtimestamp(ts, tz)
        s2 = s2.replace("T", " ").replace("Z", "")
        if "." in s2: s2 = s2.split(".")[0]
        base = s2[:19] if len(s2) >= 19 else s2[:10]
        fmt  = "%Y-%m-%d %H:%M:%S" if len(base) > 10 else "%Y-%m-%d"
        try: return tz.localize(datetime.strptime(base, fmt))
        except: return None
    return None

def end_of_day(date_str): return to_tz(date_str).replace(hour=23, minute=59, second=59)
def parse_date(dstr): return datetime.strptime(dstr, "%Y-%m-%d").date()
def fmt_date(dt): return dt.strftime("%Y-%m-%d") if dt else ""

def chunk_date_strings(start_date_str, end_date_str, max_span_days=API_MAX_RANGE_DAYS):
    s, e, cur = parse_date(start_date_str), parse_date(end_date_str), parse_date(start_date_str)
    while cur <= e:
        cur_end = min(cur + timedelta(days=max_span_days - 1), e)
        yield cur.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d")
        cur = cur_end + timedelta(days=1)

# =========================
# CLIENTE HTTP ROBUSTO
# =========================
class DMGClient:
    def __init__(self, token, base_url=BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json", "Content-Type": "application/json", "User-Agent": "kpis-report-script/final"})
        retry = Retry(total=6, connect=4, read=4, backoff_factor=0.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
    
    def paginate(self, path, params=None):
        params = dict(params or {}); params.setdefault("per_page", PAGE_SIZE)
        cursor, page_count = None, 1
        while True:
            current_params = params.copy()
            if cursor: current_params['cursor'] = cursor
            try:
                r = self.session.request("GET", self.base_url + path, params=current_params, timeout=REQUEST_TIMEOUT)
                if not r.ok: raise RuntimeError(f"HTTP {r.status_code} {self.base_url + path} -> {r.text[:400]}")
                data = r.json()
            except (requests.ReadTimeout, requests.ConnectionError) as e:
                print(f"  -> Erro de rede na página {page_count}, a tentar novamente... ({e})"); time.sleep(5); continue
            
            if not isinstance(data, dict): break
            items = data.get("data", [])
            print(f"    -> Página {page_count}: Encontrados {len(items)} itens.")
            if not items: break
            for it in items: yield it
            if data.get('has_more_pages'):
                cursor, page_count = data.get('next_cursor'), page_count + 1
                time.sleep(0.05)
            else: break

# =========================
# FUNÇÕES DE EXTRAÇÃO DE DADOS
# =========================
def _from_nested(obj, keys):
    cur = obj;
    for k in keys:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def sub_get(d, *c):
    for k in c:
        if k in d and d[k] is not None: return d[k]
    return None

def extract_net_amount(tx):
    v = _from_nested(tx, ["payment", "net"])
    if v is not None:
        try: return float(str(v).replace(",", "."))
        except (ValueError, TypeError): pass
    candidates = ["net_amount", "value", "total", "gross"]
    for k in candidates:
        v = _from_nested(tx, ["payment", k]) or _from_nested(tx, ["invoice", k]) or sub_get(tx, k)
        if v is not None:
            try: return float(str(v).replace(",", "."))
            except (ValueError, TypeError): continue
    return 0.0

def sub_created_at(sub): # <-- FUNÇÃO ADICIONADA DE VOLTA
    return from_iso_any(sub_get(sub, "created_at","started_at"))

def sub_cancelled_at(sub):
    dt = from_iso_any(sub_get(sub, "cancelled_at"))
    if dt: return dt
    if (sub_get(sub, "last_status") or "").lower() == "canceled":
        return from_iso_any(sub_get(sub, "last_status_at"))
    return None

def get_subscription_status(sub, asof_dt):
    if sub_created_at(sub) > asof_dt: return "future"
    if sub_cancelled_at(sub) and sub_cancelled_at(sub) <= asof_dt: return "canceled"
    last_status = (sub_get(sub, "last_status") or "").lower()
    if last_status in ["pastdue", "overdue", "unpaid", "delinquent"]: return "overdue"
    if last_status in ["inactive", "paused", "suspended"]: return "inactive"
    if last_status == "canceled": return "canceled"
    return "active"

# =========================
# LÓGICA PRINCIPAL
# =========================
def fetch_and_generate_reports():
    client = DMGClient(DMG_USER_TOKEN)
    
    print("\nPASSO 1: Carregando todas as assinaturas...")
    subs_all = list(fetch_with_chunks(client, "/subscriptions", "created_at_ini", "created_at_end", SUBS_CREATED_AT_INI, END_DATE))
    print(f"-> {len(subs_all)} assinaturas encontradas.")

    print("\nPASSO 2: Carregando todo o histórico de transações...")
    txs_all = list(fetch_with_chunks(client, "/transactions", "confirmed_at_ini", "confirmed_at_end", MIN_DATE_ALL, END_DATE))
    print(f"-> {len(txs_all)} transações encontradas.")

    generate_detailed_csv(subs_all, txs_all, END_DATE)
    generate_kpi_csvs(subs_all, txs_all, SUBS_CREATED_AT_INI, END_DATE)

def fetch_with_chunks(client, path, date_key_ini, date_key_end, start_date, end_date):
    print(f"Iniciando busca em '{path}' por períodos de {API_MAX_RANGE_DAYS} dias...")
    for ini, end in chunk_date_strings(start_date, end_date):
        print(f"  -> Buscando período: {ini} a {end}")
        params = {date_key_ini: ini, date_key_end: end}
        yield from client.paginate(path, params)

def generate_detailed_csv(subs, txs, end_date_str):
    print("\nGerando relatório detalhado de assinaturas (assinaturas.csv)...")
    end_dt = end_of_day(end_date_str)
    txs_by_sub_id = defaultdict(list)
    for tx in txs:
        sid = _from_nested(tx, ["subscription", "id"]) or sub_get(tx, "subscription_id")
        if sid: txs_by_sub_id[str(sid)].append(tx)

    with open(os.path.join(OUT_DIR, "assinaturas.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "data_assinatura", "data_cancelamento", "status_detalhado", "nome_assinante", "produto_oferta", "ticket_oferta", "qtd_ciclos_renovados", "ativo"])
        for s in subs:
            sid, ticket = sub_get(s, "subscription_code","code","id"), 0.0
            if not sid: continue
            sub_txs = txs_by_sub_id.get(sid, [])
            if sub_txs:
                valid_txs = sorted([tx for tx in sub_txs if _from_nested(tx, ["dates", "confirmed_at"])], key=lambda tx: _from_nested(tx, ["dates", "confirmed_at"]))
                if valid_txs: ticket = round(extract_net_amount(valid_txs[-1]), 2)
            if ticket == 0.0:
                price = sub_get(s, "value")
                ticket = round(price, 2) if price is not None else 0.0
            status = get_subscription_status(s, end_dt)
            if status == "future": continue
            writer.writerow([sid, fmt_date(sub_created_at(s)), fmt_date(sub_cancelled_at(s)), status, _from_nested(s, ["contact", "name"]), _from_nested(s, ["product", "name"]), f"{ticket:.2f}", len(sub_txs) or sub_get(s, "charged_times", 0), "TRUE" if status == "active" else "FALSE"])

def generate_kpi_csvs(subs, txs, start_date_str, end_date_str):
    print("\nGerando relatórios de KPIs (semanal e mensal)...")
    start_dt, end_dt = to_tz(start_date_str), to_tz(end_date_str)
    
    extract_confirmed_at_from_tx = lambda t: from_iso_any(_from_nested(t, ["dates", "confirmed_at"]))

    # Monthly KPIs
    monthly_kpis = []
    month_iter = start_dt.replace(day=1)
    while month_iter <= end_dt:
        month_ini = month_iter
        month_end = (month_ini + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        novas = len([s for s in subs if sub_created_at(s) and month_ini <= sub_created_at(s) <= month_end])
        cancelados = len([s for s in subs if sub_cancelled_at(s) and month_ini <= sub_cancelled_at(s) <= month_end])
        receita = sum([extract_net_amount(t) for t in txs if extract_confirmed_at_from_tx(t) and month_ini <= extract_confirmed_at_from_tx(t) <= month_end])
        ativos_fim_mes = len([s for s in subs if sub_created_at(s) <= month_end and (not sub_cancelled_at(s) or sub_cancelled_at(s) > month_end)])
        monthly_kpis.append({"month": month_ini.strftime("%Y-%m"), "novas_assinaturas_brutas": novas, "cancelamentos_brutos": cancelados, "receita": round(receita, 2), "ticket_medio": round(receita / ativos_fim_mes, 2) if ativos_fim_mes > 0 else 0})
        month_iter = (month_iter + timedelta(days=32)).replace(day=1)
    
    with open(os.path.join(OUT_DIR, "monthly_kpis.csv"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["month", "novas_assinaturas_brutas", "cancelamentos_brutos", "receita", "ticket_medio"])
        writer.writeheader(); writer.writerows(monthly_kpis)

    # Weekly KPIs
    weekly_kpis = []
    day_iter = start_dt - timedelta(days=start_dt.weekday())
    while day_iter <= end_dt:
        week_start, week_end = day_iter, day_iter + timedelta(days=6)
        novas = len([s for s in subs if sub_created_at(s) and week_start <= sub_created_at(s) <= week_end])
        cancelados = len([s for s in subs if sub_cancelled_at(s) and week_start <= sub_cancelled_at(s) <= week_end])
        weekly_kpis.append({"week_start": fmt_date(week_start), "week_end": fmt_date(week_end), "novas_assinaturas_brutas": novas, "cancelamentos_brutos": cancelados})
        day_iter += timedelta(days=7)

    with open(os.path.join(OUT_DIR, "weekly_kpis.csv"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["week_start", "week_end", "novas_assinaturas_brutas", "cancelamentos_brutos"])
        writer.writeheader(); writer.writerows(weekly_kpis)

if __name__ == "__main__":
    print("Iniciando script de extração de dados...")
    fetch_and_generate_reports()
    print("\nScript de extração de dados concluído com sucesso.")