# gerar_dashboard.py
# -*- coding: utf-8 -*-
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os

# =========================
# CONFIGURAÇÃO
# =========================
OUT_DIR = './out'
DASHBOARD_DIR = './dashboard'
ASSINATURAS_CSV = os.path.join(OUT_DIR, 'assinaturas.csv')
OUTPUT_HTML = os.path.join(DASHBOARD_DIR, 'index.html')

# =========================
# 1. CARREGAMENTO E PROCESSAMENTO DOS DADOS
# =========================
def process_data_for_dashboard():
    print("A carregar e processar dados do CSV...")
    try:
        df = pd.read_csv(ASSINATURAS_CSV, dtype={'ativo': str})
    except FileNotFoundError:
        print(f"ERRO: Ficheiro '{ASSINATURAS_CSV}' não encontrado. Execute o 'main.py' primeiro.")
        return None

    df['data_assinatura'] = pd.to_datetime(df['data_assinatura'])
    df['data_cancelamento'] = pd.to_datetime(df['data_cancelamento'], errors='coerce')
    
    data_for_js = df.to_json(orient='records', date_format='iso')
    print("Processamento de dados concluído.")
    return data_for_js

# =========================
# 2. GERAÇÃO DOS FICHEIROS DO SITE
# =========================
def write_dashboard_files(json_data):
    print("A gerar ficheiros do dashboard (HTML, CSS, JS)...")
    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    # --- Conteúdo do HTML ---
    html_content = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Assinaturas</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/material_blue.css">
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <main class="container">
        <h1>Dashboard de Assinaturas</h1>
        <p id="last-update" class="footer"></p>
        
        <h2>Visão Geral (Global)</h2>
        <div id="global-kpi-grid" class="kpi-grid"></div>

        <div class="filters">
            <input type="text" id="date-range-picker" placeholder="Selecione um período">
        </div>
        
        <h2>Métricas da Coorte Selecionada</h2>
        <div id="period-kpi-grid" class="kpi-grid"></div>
        
        <h2>Tendências da Coorte</h2>
        <div class="chart-grid">
            <div class="chart-container">
                <canvas id="retentionChart"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="mrrChart"></canvas>
            </div>
        </div>
    </main>
    <script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
    <script src="https://npmcdn.com/flatpickr/dist/l10n/pt.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="script.js"></script>
</body>
</html>
    """

    # --- Conteúdo do CSS ---
    css_content = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; margin: 0; padding: 25px; background-color: #f0f2f5; color: #1c1e21; }
.container { max-width: 1400px; margin: 0 auto; }
h1, h2 { text-align: center; color: #1c1e21; margin-bottom: 20px; }
h2 { margin-top: 40px; border-bottom: 1px solid #ddd; padding-bottom: 10px; font-size: 1.2em; color: #333; }
p.footer { text-align: center; color: #606770; font-size: 0.9em; margin-top: -15px; margin-bottom: 30px; }
.filters { display: flex; justify-content: center; margin-bottom: 30px; }
/* Estilo para o container do Flatpickr, que agora é um input */
#date-range-picker { text-align: center; font-size: 1.1em; padding: 10px; border-radius: 8px; border: 1px solid #ccc; width: 300px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
.kpi-card { background-color: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }
.kpi-title { font-size: 0.9em; font-weight: 600; color: #606770; margin-bottom: 10px; }
.kpi-value { font-size: 2.2em; font-weight: 700; color: #1c1e21; }
.chart-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 20px; }
.chart-container { background-color: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); padding: 15px; }
@media (max-width: 900px) { .chart-grid { grid-template-columns: 1fr; } }
    """

    # --- Conteúdo do JavaScript ---
    js_content = f"""
const rawData = {json_data};
const data = rawData.map(d => ({{
    ...d,
    ativo: String(d.ativo).trim().toUpperCase() === 'TRUE',
    data_assinatura: new Date(d.data_assinatura),
    data_cancelamento: d.data_cancelamento ? new Date(d.data_cancelamento) : null
}}));

let retentionChart = null;
let mrrChart = null;

function updateDashboard(startDate, endDate) {{
    endDate.setHours(23, 59, 59, 999);
    const cohort = data.filter(d => d.data_assinatura >= startDate && d.data_assinatura <= endDate);
    
    const kpiGrid = document.getElementById('period-kpi-grid');
    kpiGrid.innerHTML = '';

    if (cohort.length === 0) {{
        kpiGrid.innerHTML = '<p style="text-align: center; width: 100%;">Nenhuma assinatura iniciada no período selecionado.</p>';
        if(retentionChart) retentionChart.destroy();
        if(mrrChart) mrrChart.destroy();
        return;
    }}

    const cohortSize = cohort.length;
    const cohortCancelados = cohort.filter(d => d.data_cancelamento);
    const cohortAtivos = cohort.filter(d => d.ativo);
    const churnRate = (cohortCancelados.length / cohortSize) * 100;
    const lifetimeDays = cohortCancelados.map(d => (d.data_cancelamento - d.data_assinatura) / (1000 * 60 * 60 * 24));
    const fpcDias = lifetimeDays.length > 0 ? lifetimeDays.reduce((a, b) => a + b, 0) / lifetimeDays.length : 0;
    const ticketMedio = cohort.length > 0 ? cohort.reduce((sum, d) => sum + d.ticket_oferta, 0) / cohortSize : 0;
    const ltv = ticketMedio * (fpcDias / 30.44);
    const mrrAtualDaCoorte = cohortAtivos.reduce((sum, d) => sum + d.ticket_oferta, 0);

    const kpis = {{
        "Assinantes na Coorte": cohortSize, "Churn Rate da Coorte": `${{churnRate.toFixed(2)}}%`,
        "MRR Atual da Coorte": `R$ ${{mrrAtualDaCoorte.toFixed(2)}}`, "LTV da Coorte": `R$ ${{ltv.toFixed(2)}}`,
        "Ticket Médio da Coorte": `R$ ${{ticketMedio.toFixed(2)}}`, "Tempo até Cancelar": `${{fpcDias.toFixed(1)}} dias`
    }};

    for (const [key, value] of Object.entries(kpis)) {{
        const card = document.createElement('div');
        card.className = 'kpi-card';
        card.innerHTML = `<div class="kpi-title">${{key}}</div><div class="kpi-value">${{value}}</div>`;
        kpiGrid.appendChild(card);
    }}

    const cohortLifetimes = cohort.map(sub => {{
        const endDate = sub.data_cancelamento || new Date();
        return Math.floor(((endDate - sub.data_assinatura) / (1000 * 60 * 60 * 24)) / 30.44);
    }});
    const maxMonths = cohortLifetimes.length > 0 ? Math.max(...cohortLifetimes) + 1 : 1;
    
    const firstMonth = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
    const chartLabels = Array.from(Array(maxMonths).keys()).map(i => {{
        const date = new Date(firstMonth.getFullYear(), firstMonth.getMonth() + i, 1);
        return date.toLocaleDateString('pt-BR', {{ year: 'numeric', month: 'short' }});
    }});
    
    const retentionData = Array(maxMonths).fill(0);
    cohort.forEach(sub => {{
        const endDate = sub.data_cancelamento || new Date();
        const lifetimeMonths = Math.floor(((endDate - sub.data_assinatura) / (1000 * 60 * 60 * 24)) / 30.44);
        for (let i = 0; i <= lifetimeMonths && i < maxMonths; i++) retentionData[i]++;
    }});
    const retentionPercentage = retentionData.map(count => (count / cohortSize) * 100);
    
    if (retentionChart) retentionChart.destroy();
    retentionChart = new Chart(document.getElementById('retentionChart'), {{
        type: 'line',
        data: {{
            labels: chartLabels,
            datasets: [{{ label: '% de Retenção da Coorte', data: retentionPercentage, borderColor: '#4bc0c0', backgroundColor: 'rgba(75, 192, 192, 0.2)', fill: true, tension: 0.1 }}]
        }},
        options: {{ responsive: true, plugins: {{ title: {{ display: true, text: 'Curva de Retenção da Coorte' }} }} }}
    }});

    const mrrTimeline = Array(maxMonths).fill(0);
    cohort.forEach(sub => {{
        const endMonth = sub.data_cancelamento ? Math.floor(((sub.data_cancelamento - sub.data_assinatura) / (1000 * 60 * 60 * 24)) / 30.44) : maxMonths -1;
        for (let i = 0; i <= endMonth && i < maxMonths; i++) mrrTimeline[i] += sub.ticket_oferta;
    }});

    if (mrrChart) mrrChart.destroy();
    mrrChart = new Chart(document.getElementById('mrrChart'), {{
        type: 'line',
        data: {{
            labels: chartLabels,
            datasets: [{{ label: 'MRR da Coorte (R$)', data: mrrTimeline, borderColor: '#36a2eb', backgroundColor: 'rgba(54, 162, 235, 0.2)', fill: true, tension: 0.1 }}]
        }},
        options: {{ responsive: true, plugins: {{ title: {{ display: true, text: 'MRR Gerado pela Coorte ao Longo do Tempo' }} }} }}
    }});
}}

document.addEventListener('DOMContentLoaded', () => {{
    const totalAtivos = data.filter(d => d.ativo).length;
    const totalCancelados = data.filter(d => !d.ativo).length;
    
    document.getElementById('global-kpi-grid').innerHTML = `
        <div class="kpi-card"><div class="kpi-title">Total de Assinantes Ativos</div><div class="kpi-value">${{totalAtivos}}</div></div>
        <div class="kpi-card"><div class="kpi-title">Total de Assinantes Cancelados</div><div class="kpi-value">${{totalCancelados}}</div></div>
    `;

    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    flatpickr("#date-range-picker", {{
        mode: "range",
        dateFormat: "Y-m-d",
        defaultDate: [thirtyDaysAgo, "today"],
        "locale": "pt", // Tradução para português
        onChange: function(selectedDates) {{
            if (selectedDates.length === 2) {{
                updateDashboard(selectedDates[0], selectedDates[1]);
            }}
        }},
        onReady: function(selectedDates, dateStr, instance) {{
            // Carga inicial dos dados com o período padrão
            if(instance.selectedDates.length === 2) {{
                updateDashboard(instance.selectedDates[0], instance.selectedDates[1]);
            }}
        }}
    }});

    document.getElementById('last-update').textContent = `Dados atualizados em: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}`;
}});
    """

    with open(os.path.join(DASHBOARD_DIR, 'index.html'), 'w', encoding='utf-8') as f: f.write(html_content)
    with open(os.path.join(DASHBOARD_DIR, 'style.css'), 'w', encoding='utf-8') as f: f.write(css_content)
    with open(os.path.join(DASHBOARD_DIR, 'script.js'), 'w', encoding='utf-8') as f: f.write(js_content)
    
    print("Ficheiros do dashboard gerados com sucesso.")

if __name__ == "__main__":
    json_data_for_js = process_data_for_dashboard()
    if json_data_for_js:
        write_dashboard_files(json_data_for_js)
        print(f"\\n--- SUCESSO ---")
        print(f"Dashboard de Coortes gerado!")
        print(f"Abra o ficheiro '{os.path.abspath(os.path.join(DASHBOARD_DIR, 'index.html'))}' no seu navegador.")