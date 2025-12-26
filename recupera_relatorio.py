import pickle
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
from DB_customers import Database


def gerar_grafico_oleo():
    with open("resultado.pkl", "rb") as f:
        df = pickle.load(f)

    # Gráfico de linhas
    plt.figure(figsize=(10, 6))
    plt.plot(df['ano'], df['total_vendas_em_litros'], marker='o')
    plt.xlabel('Ano')
    plt.ylabel('Total de Vendas em Litros')
    plt.title('Vendas de lubrificantes ao longo dos anos')

    # Forçar todos os anos a aparecerem no eixo x
    plt.xticks(df['ano'], rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def totalizar_filtros() -> None:
    CAMINHO = Path("filtros.csv")
    df_filtros = pd.read_csv(CAMINHO, encoding='utf-8')
    df_distinct = df_filtros.drop_duplicates()
    df_distinct['DATA'] = pd.to_datetime(df_distinct['DATA'])
    df_distinct['ANO'] = df_distinct['DATA'].dt.year
    df = df_distinct.groupby('ANO').agg({'QUANTIDADE': 'sum'}).reset_index()
    print('*********************************')
    print("FUNÇÃO: totalizar_filtros")
    print('*********************************')
    print(df)
    print('\n')

    # Gráfico de linhas
    #plt.figure(figsize=(10, 6))
    #plt.plot(df['ANO'], df['QUANTIDADE'], marker='o')
    #plt.xlabel('Ano')
    #plt.ylabel('Totald de vendas')
    #plt.title('Vendas de filtros ao longo dos anos')

    # Forçar todos os anos a aparecerem no eixo x
    #plt.xticks(df['ANO'], rotation=45)
    #plt.grid(True, alpha=0.3)
    #plt.tight_layout()
    #plt.show()
    return None


def totalizar_filtros_() -> None:
    SQL_ = """
        SELECT DISTINCT
	        p.DATAFATURA as DATA,
	        p3.DESCRICAO,
	        p2.QUANTIDADE
        FROM PEDIDOC p
        INNER JOIN PEDIDOI p2
	      
            ON p.CODPEDIDO = p2.CODPEDIDO
        INNER JOIN PRODUTO p3
	        ON p2.CODPROD = p3.CODPROD
        INNER JOIN GRUPROD p4
	        ON p3.CODGRUPO = p4.CODGRUPO
        WHERE
	        p.DATAFATURA >= '2016-01-01' AND p.DATAFATURA <= CURRENT_TIMESTAMP - 1
	        AND p3.CODGRUPO = 003
	        AND p.FATURADO = 'S'
            AND p.CODPEDIDO NOT LIKE '101%' AND p.CODPEDIDO NOT LIKE '100%';
    """
    # Importação das funções de conexão ao banco de dados
    dados_db = Database(
        host='resulthserv',
        database='c:\\ResWinCS\\Banco\\RESULTH.FB',
        user='sysdba',
        password='masterkey'
    )
    dados_resulth = dados_db.execute_query(SQL_)
    resultado = pd.DataFrame(dados_resulth, columns=['DATA', 'DESCRICAO', 'QUANTIDADE'])
    resultado['DATA'] = pd.to_datetime(resultado['DATA'])
    resultado['ANO'] = resultado['DATA'].dt.year
    df = resultado.groupby('ANO').agg({'QUANTIDADE': 'sum'}).reset_index()
    print('*********************************')
    print("FUNÇÃO: totalizar_filtros_")
    print('*********************************')
    print(df)
    print('\n')
    return None


if __name__ == "__main__":
    #oleo = gerar_grafico_oleo()

    ######################################
    # Usando o arquivo .csv
    totalizar_filtros()

    # Consultando direto do banco de dados
    totalizar_filtros_()
    ######################################

    #Gerar gráfico na mesma página
    #fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
    #axs[0].plot(oleo['ano'], oleo['total_vendas_em_litros'], color='blue')
    #axs[1].plot(filtro['DATA'], filtro['QUANTIDADE'], color='red')
    #plt.tight_layout()
    #plt.show()

