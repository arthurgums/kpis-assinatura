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

# Carrega as variáveis do ficheiro .env para o ambiente
load_dotenv()

# =========================
# CONFIG
# =========================

BASE_URL = "https://digitalmanager.guru/api/v2"

# Lê o token do ambiente (carregado a partir do .env). Se não existir, o script irá falhar.
DMG_USER_TOKEN = os.getenv("DMG_USER_TOKEN")
if not DMG_USER_TOKEN:
    raise ValueError("O token DMG_USER_TOKEN não foi encontrado. Verifique se o ficheiro .env existe e está configurado corretamente.")

END_DATE   = os.getenv("END_DATE",   datetime.now().strftime("%Y-%m-%d"))

OUT_DIR         = "./out"
os.makedirs(OUT_DIR, exist_ok=True)

DMG_TZ = "America/Sao_Paulo"
PAGE_SIZE = 200
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "25"))
READ_TIMEOUT    = int(os.getenv("READ_TIMEOUT", "120"))
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

API_MAX_RANGE_DAYS = 180
MIN_DATE_ALL = datetime.now().strftime("%Y-01-01")
SUBS_CREATED_AT_INI = MIN_DATE_ALL

# =========================
# TEMPO / TZ
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
        ts = float(s);  ts = ts/1000.0 if ts > 1e12 else ts
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
    s = parse_date(start_date_str); e = parse_date(end_date_str); cur = s
    while cur <= e:
        cur_end = min(cur + timedelta(days=max_span_days - 1), e)
        yield cur.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d")
        cur = cur_end + timedelta(days=1)

# =========================
# HTTP CLIENT (robusto)
# =========================

class DMGClient:
    def __init__(self, token, base_url=BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}", "Accept": "application/json",
            "Content-Type": "application/json", "User-Agent": "kpis-report-script/15.0"
        })
        retry = Retry(total=6, connect=4, read=4, backoff_factor=0.8, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _full_url(self, path_or_url):
        return path_or_url if str(path_or_url).startswith("http") else (self.base_url + path_or_url)

    def req(self, method, path_or_url, params=None):
        url = self._full_url(path_or_url)
        last_err = None
        for attempt in range(6):
            try:
                r = self.session.request(method, url, params=params, timeout=REQUEST_TIMEOUT)
                if not r.ok: raise RuntimeError(f"HTTP {r.status_code} {url} -> {r.text[:400]}")
                try: return r.json()
                except: return r.text
            except (requests.ReadTimeout, requests.ConnectionError, requests.Timeout) as e:
                last_err = e; time.sleep(0.7 * (2 ** attempt)); continue
        raise last_err if last_err else RuntimeError("Unknown request error")

    def paginate(self, path, params=None):
        params = dict(params or {}); params.setdefault("per_page", PAGE_SIZE)
        cursor, page_count = None, 1
        
        while True:
            current_params = params.copy()
            if cursor: current_params['cursor'] = cursor
            data = self.req("GET", path, current_params)
            if not isinstance(data, dict): break
            items = data.get("data", [])
            print(f"    -> Página {page_count}: Encontrados {len(items)} itens.")
            if not items: break
            for it in items: yield it
            if data.get('has_more_pages') == True:
                cursor, page_count = data.get('next_cursor'), page_count + 1
                time.sleep(0.05)
            else: break

# =========================
# HELPERS
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

def sub_code(sub): return sub_get(sub, "subscription_code","code","id") or ""
def sub_contact_name(sub): return _from_nested(sub, ["contact", "name"]) or ""
def sub_created_at(sub): return from_iso_any(sub_get(sub, "created_at","started_at"))

def sub_cancelled_at(sub):
    dt = from_iso_any(sub_get(sub, "cancelled_at"))
    if dt: return dt
    
    last_status = (sub_get(sub, "last_status") or "").lower()
    if last_status == "canceled":
        return from_iso_any(sub_get(sub, "last_status_at"))
    return None

def charge_count(sub): return int(sub_get(sub, "charged_times") or 0)
def extract_subscription_id_from_tx(tx): return _from_nested(tx, ["subscription", "id"]) or sub_get(tx, "subscription_id")
def extract_confirmed_at(tx): return from_iso_any(_from_nested(tx, ["dates", "confirmed_at"]))
def sub_offer_name(sub): return _from_nested(sub, ["product", "name"]) or ""
def sub_offer_price_field(sub): return sub_get(sub, "value")

# =========================
# LOADERS
# =========================

def fetch_with_chunks(client, path, date_key_ini, date_key_end, start_date, end_date):
    seen_ids, results = set(), []
    print(f"Iniciando busca em '{path}' por períodos de {API_MAX_RANGE_DAYS} dias...")
    for ini, end in chunk_date_strings(start_date, end_date):
        print(f"  -> Buscando período: {ini} a {end}")
        params = {date_key_ini: ini, date_key_end: end}
        for item in client.paginate(path, params):
            item_id = item.get("id")
            if item_id and item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item_id)
    return results

# =========================
# LÓGICA DE STATUS
# =========================

def get_subscription_status(sub, asof_dt):
    if sub_created_at(sub) and sub_created_at(sub) > asof_dt:
        return "future"
    
    if sub_cancelled_at(sub) and sub_cancelled_at(sub) <= asof_dt:
        return "canceled"

    last_status = (sub_get(sub, "last_status") or "").lower()
    if last_status in ["pastdue", "overdue", "unpaid", "delinquent"]:
        return "overdue"
    if last_status in ["inactive", "paused", "suspended"]:
        return "inactive"
    if last_status == "canceled":
        return "canceled"
    
    return "active"

# =========================
# GERAÇÃO DO RELATÓRIO CSV
# =========================

def generate_assinaturas_report(subs, txs, end_date_str):
    print("\nGerando relatório detalhado de assinaturas...")
    end_dt = end_of_day(end_date_str)
    txs_by_sub_id = defaultdict(list)
    for tx in txs:
        sid = extract_subscription_id_from_tx(tx)
        if sid: txs_by_sub_id[str(sid)].append(tx)

    out_path = os.path.join(OUT_DIR, "assinaturas.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "data_assinatura", "data_cancelamento", "status_detalhado", "nome_assinante", "produto_oferta", "ticket_oferta", "qtd_ciclos_renovados", "ativo"])
        
        for s in subs:
            sid, ticket = sub_code(s), 0.0
            if not sid: continue

            sub_txs = txs_by_sub_id.get(sid, [])
            if sub_txs:
                valid_txs = sorted([tx for tx in sub_txs if extract_confirmed_at(tx)], key=lambda tx: extract_confirmed_at(tx))
                if valid_txs:
                    last_tx = valid_txs[-1]
                    ticket = round(extract_net_amount(last_tx), 2)
            
            if ticket == 0.0:
                price_from_offer = sub_offer_price_field(s)
                ticket = round(price_from_offer, 2) if price_from_offer is not None else 0.0

            status = get_subscription_status(s, end_dt)
            if status == "future": continue
            
            is_active = (status == "active")

            writer.writerow([
                sid,
                fmt_date(sub_created_at(s)),
                fmt_date(sub_cancelled_at(s)),
                status,
                sub_contact_name(s),
                sub_offer_name(s),
                f"{ticket:.2f}",
                len(sub_txs) or charge_count(s),
                "TRUE" if is_active else "FALSE"
            ])
    
    print(f"Relatório de assinaturas salvo em: {out_path}")
    return out_path

# =========================
# CÁLCULO DE KPIS
# =========================

def calculate_and_export_kpis(subs, txs, start_date_str, end_date_str):
    print("\nCalculando e exportando KPIs...")
    start_dt = to_tz(start_date_str)
    end_dt = to_tz(end_date_str)

    # --- KPI MENSAL ---
    monthly_kpis = []
    current_month = start_dt.replace(day=1)
    while current_month <= end_dt:
        month_ini = current_month
        next_month_ini = (month_ini.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month_ini - timedelta(days=1)
        
        novas = len([s for s in subs if sub_created_at(s) and month_ini <= sub_created_at(s) <= month_end])
        cancelados = len([s for s in subs if sub_cancelled_at(s) and month_ini <= sub_cancelled_at(s) <= month_end])
        receita = sum([extract_net_amount(t) for t in txs if extract_confirmed_at(t) and month_ini <= extract_confirmed_at(t) <= month_end])
        ativos_fim_mes = len([s for s in subs if sub_created_at(s) and sub_created_at(s) <= month_end and (not sub_cancelled_at(s) or sub_cancelled_at(s) > month_end)])
        
        ticket_medio = receita / ativos_fim_mes if ativos_fim_mes > 0 else 0
        
        monthly_kpis.append({
            "month": month_ini.strftime("%Y-%m"),
            "novas_assinaturas_brutas": novas,
            "cancelamentos_brutos": cancelados,
            "receita": round(receita, 2),
            "ticket_medio": round(ticket_medio, 2)
        })
        current_month = next_month_ini

    monthly_path = os.path.join(OUT_DIR, "monthly_kpis.csv")
    with open(monthly_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["month", "novas_assinaturas_brutas", "cancelamentos_brutos", "receita", "ticket_medio"])
        writer.writeheader()
        writer.writerows(monthly_kpis)

    # --- KPI SEMANAL ---
    weekly_kpis = []
    current_day = start_dt
    while current_day <= end_dt:
        if current_day.weekday() == 0:
            week_start = current_day
            week_end = current_day + timedelta(days=6)
            
            novas = len([s for s in subs if sub_created_at(s) and week_start <= sub_created_at(s) <= week_end])
            cancelados = len([s for s in subs if sub_cancelled_at(s) and week_start <= sub_cancelled_at(s) <= week_end])

            weekly_kpis.append({
                "week_start": fmt_date(week_start),
                "week_end": fmt_date(week_end),
                "novas_assinaturas_brutas": novas,
                "cancelamentos_brutos": cancelados
            })
        current_day += timedelta(days=1)
        
    weekly_path = os.path.join(OUT_DIR, "weekly_kpis.csv")
    with open(weekly_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["week_start", "week_end", "novas_assinaturas_brutas", "cancelamentos_brutos"])
        writer.writeheader()
        writer.writerows(weekly_kpis)
        
    print(f"KPIs exportados para '{monthly_path}' e '{weekly_path}'.")
    return monthly_path, weekly_path

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    print("Iniciando script de extração de dados...")
    
    client = DMGClient(DMG_USER_TOKEN)
    
    print("\nPASSO 1 de 2: Carregando todas as assinaturas...")
    subs_all = fetch_with_chunks(client, "/subscriptions", "created_at_ini", "created_at_end", SUBS_CREATED_AT_INI, END_DATE)
    print(f"-> {len(subs_all)} assinaturas encontradas.")

    print("\nPASSO 2 de 2: Carregando todo o histórico de transações...")
    txs_all = fetch_with_chunks(client, "/transactions", "confirmed_at_ini", "confirmed_at_end", MIN_DATE_ALL, END_DATE)
    print(f"-> {len(txs_all)} transações encontradas.")

    generate_assinaturas_report(subs_all, txs_all, END_DATE)
    calculate_and_export_kpis(subs_all, txs_all, SUBS_CREATED_AT_INI, END_DATE)

    print("\nScript de extração de dados concluído com sucesso.")