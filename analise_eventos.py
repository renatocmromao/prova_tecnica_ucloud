import pandas as pd
import os

def gerar_csv_exemplo():
    """Gera um arquivo CSV de exemplo para a análise."""
    dados = {
        'user_id': ['u1', 'u2', 'u1', 'u3', 'u2', 'u1', 'u2', 'u4', 'u3', 'u1', 'u2', 'u4'],
        'timestamp': pd.to_datetime([
            '2025-07-16 10:00:00', '2025-07-16 10:01:00', '2025-07-16 10:02:00',
            '2025-07-16 10:03:00', '2025-07-16 10:04:00', '2025-07-16 10:05:00',
            '2025-07-16 10:06:00', '2025-07-16 10:07:00', '2025-07-16 10:08:00',
            '2025-07-16 10:09:00', '2025-07-16 10:10:00', '2025-07-16 10:11:00'
        ]),
        'event_type': ['view', 'view', 'click', 'view', 'purchase', 'purchase', 'view', 'view', 'purchase', 'view', 'click', 'purchase'],
        'product_id': ['p101', 'p202', 'p101', 'p303', 'p202', 'p101', 'p404', 'p101', 'p303', 'p101', 'p404', 'p101'],
        'price': [None, None, None, None, 150.00, 99.90, None, None, 25.50, None, None, 99.90]
    }
    df_exemplo = pd.DataFrame(dados)
    df_exemplo.to_csv('events.csv', index=False)
    print("Arquivo 'events.csv' de exemplo gerado com sucesso.")

def analisar_eventos(caminho_csv: str):
    """
    Lê um CSV de eventos, calcula métricas de e-commerce e salva um resumo.
    
    Args:
        caminho_csv (str): O caminho para o arquivo CSV de entrada.
    """
    # 1. LEIA O CSV
    print(f"\nLendo dados de '{caminho_csv}'...")
    try:
        df = pd.read_csv(caminho_csv, parse_dates=['timestamp'])
    except FileNotFoundError:
        print(f"Erro: Arquivo '{caminho_csv}' não encontrado.")
        return

    print("Dados lidos com sucesso. Iniciando cálculos...")

    # 2. CALCULE AS MÉTRICAS

    # Filtra apenas os eventos de compra para otimizar os cálculos seguintes
    compras_df = df[df['event_type'] == 'purchase'].copy()

    # ■ Total de compras por usuário
    total_compras_por_usuario = compras_df.groupby('user_id')['event_type'].count()
    total_compras_por_usuario.name = 'total_purchases'

    # ■ Valor total gasto por usuário
    valor_gasto_por_usuario = compras_df.groupby('user_id')['price'].sum()
    valor_gasto_por_usuario.name = 'total_spent'

    # ■ Produto mais comprado por quantidade
    produto_mais_comprado = compras_df['product_id'].mode()[0]
    print(f"\n--- Métrica Global ---")
    print(f"Produto mais comprado por quantidade: {produto_mais_comprado}")
    
    # ■ Conversão por tipo de evento (compras / visualizações) por usuário
    # Agrupa por usuário e tipo de evento para contar as ocorrências de cada um
    eventos_por_usuario = df.groupby(['user_id', 'event_type']).size().unstack(fill_value=0)
    
    # Garante que as colunas 'purchase' e 'view' existam para evitar erros
    if 'purchase' not in eventos_por_usuario:
        eventos_por_usuario['purchase'] = 0
    if 'view' not in eventos_por_usuario:
        eventos_por_usuario['view'] = 0
        
    # Calcula a conversão, tratando a divisão por zero (usuários sem visualizações)
    eventos_por_usuario['conversion_rate'] = (eventos_por_usuario['purchase'] / eventos_por_usuario['view']).fillna(0)

    # 3. SALVE UM RESUMO EM UM NOVO CSV
    
    # Consolida as métricas por usuário em um único DataFrame
    summary_df = pd.concat([
        total_compras_por_usuario, 
        valor_gasto_por_usuario, 
        eventos_por_usuario['conversion_rate']
    ], axis=1).fillna(0)

    # Reorganiza as colunas e reseta o índice para que 'user_id' se torne uma coluna
    summary_df = summary_df.reset_index()
    
    caminho_saida = 'user_summary.csv'
    summary_df.to_csv(caminho_saida, index=False)
    
    print("\n--- Resumo por Usuário ---")
    print(summary_df)
    print(f"\nAnálise concluída. Resumo salvo em '{caminho_saida}'.")


if __name__ == '__main__':
    # Gera o arquivo de exemplo para garantir que o script possa ser executado
    gerar_csv_exemplo()
    
    # Executa a função principal de análise
    analisar_eventos('events.csv')