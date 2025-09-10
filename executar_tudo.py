# executar_tudo.py
# -*- coding: utf-8 -*-
import subprocess
import sys
import os
from datetime import datetime

def executar_script(nome_script):
    """Executa um script Python e verifica se houve erros."""
    print("-" * 50)
    print(f"A EXECUTAR: {nome_script}")
    print("-" * 50)
    
    python_executable = sys.executable
    
    try:
        processo = subprocess.Popen(
            [python_executable, nome_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace' 
        )
        
        for linha in processo.stdout:
            print(linha, end='')

        processo.wait()

        if processo.returncode != 0:
            print("\n!!!!!! ERRO AO EXECUTAR SCRIPT !!!!!!")
            print(f"O script '{nome_script}' terminou com o código de erro {processo.returncode}.")
            print("Saída do erro:")
            print(processo.stderr.read())
            return False

        print(f"\n--- SUCESSO: {nome_script} concluído sem erros. ---")
        return True
        
    except FileNotFoundError:
        print(f"\n!!!!!! ERRO: O ficheiro '{nome_script}' não foi encontrado. !!!!!!")
        return False
    except Exception as e:
        print(f"\n!!!!!! Ocorreu um erro inesperado: {e} !!!!!!")
        return False

if __name__ == "__main__":
    start_time = datetime.now()
    print("="*50)
    print("INICIANDO PROCESSO COMPLETO DE GERAÇÃO DE KPIS")
    print(f"Iniciado em: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*50)

    scripts = ['main.py', 'gerar_dashboard.py']

    for script in scripts:
        if not executar_script(script):
            print("\nO processo foi interrompido devido a um erro.")
            break
    else:
        end_time = datetime.now()
        dashboard_path = os.path.abspath(os.path.join('docs', 'index.html'))
        print("\n\nPROCESSO FINALIZADO COM SUCESSO!")
        print(f"Concluído em: {end_time.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Duração total: {end_time - start_time}")
        print(f"\nPode abrir o seu dashboard atualizado em: {dashboard_path}")