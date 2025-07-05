import fdb
from datetime import date, timedelta
import pandas as pd
from pprint import pprint


def db_resulth() -> tuple:

    with fdb.connect(
            host='resulthserv', database='c:\\ResWinCS\\Banco\\RESULTH.FB',
            user='sysdba', password='masterkey') as conn:

        cursor = conn.cursor()
        one_year = timedelta(days=365)
        two_years = timedelta(days=730)
        today = date.today()

        # Consulta ao total faturado em 12 meses
        cursor.execute(
            f"""SELECT SUM(p.TOTALPEDIDO)
            FROM PEDIDOC p
            WHERE p.DATAFATURA BETWEEN '{today - one_year}' AND '{today}'
            AND p.TIPOPEDIDO = 55 AND FATURADO = 'S' AND p.TIPODAV <> 'O'
            """
        )
        total_faturamento = cursor.fetchall()

        # Consulta ao saldo VENCIDO do Contas a Receber
        cursor.execute(
            f"""SELECT SUM(d.VALORDOCTO) - SUM(d.VALORPAGO) AS Resultado
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '2025-02-01' AND '{today}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            """
        )
        total_vencidos = cursor.fetchall()

        # Consulta ao saldo a VENCER do Contas a Receber
        cursor.execute(
            f"""SELECT SUM(d.VALORDOCTO) - SUM(d.VALORPAGO) AS Resultado
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '{today}' AND '{today + two_years}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            """
        )
        total_a_vencer = cursor.fetchall()
    return total_faturamento[0][0], total_vencidos[0][0], total_a_vencer[0][0]


def db_resulth_balance():
    with fdb.connect(
            host='resulthserv', database='c:\\ResWinCS\\Banco\\RESULTH.FB',
            user='sysdba', password='masterkey') as conn:
        cursor = conn.cursor()
        today = date.today()
        one_day = timedelta(days=1)
        large_day = timedelta(days=180)

        cursor.execute(
            f"""SELECT d.CODCLIENTE, c.NOME , d.DT_VENCIMENTO, d.VALORDOCTO, (d.VALORDOCTO - d.VALORPAGO) AS SALDO, DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIFF_DATA
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '{today - large_day}' AND '{today - one_day}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            AND (d.VALORDOCTO - d.VALORPAGO) > 0 AND (d.VALORDESC + d.VALORPAGO) <> d.VALORDOCTO
            AND d.VALORPAGO = 0
            ORDER BY d.DT_VENCIMENTO
            """
        )
        return cursor.fetchall()


if __name__ == "__main__":
    # Formatação numérica de duas casas decimais
    pd.options.display.float_format = '{:.2f}'.format

    # Criar o DataFrame no Pandas
    df = pd.DataFrame(db_resulth_balance(), columns=['COD', 'NOME', 'DATA', 'VLRDOCTO', 'SALDO', 'DIFF_DATA'])

    # realiza o produto da coluna SALDO com a coluna DIFF_DATA
    prod_saldo_diffdata = []
    for linha in df.itertuples():
        prod_saldo_diffdata.append(linha.SALDO * linha.DIFF_DATA)
    df['PRODUTO'] = prod_saldo_diffdata
    #print(df)

    # Realiza o agrupamento dos clientes
    df_gruouped = df.groupby(['COD', 'NOME']).agg({
        'SALDO': 'sum',
        'PRODUTO': 'sum'
    }).reset_index()

    # Cálculo do PMR por cliente
    pmr_customers = []
    for row in df_gruouped.itertuples():
        pmr_customers.append(row.PRODUTO / row.SALDO)
    df_gruouped['PMR'] = pmr_customers
    df_finally = df_gruouped[['COD', 'NOME', 'SALDO', 'PMR']]
    print(df_finally)
    gp = df_finally.loc[df_finally['PMR'] > 27].reset_index()
    pprint(gp[['COD', 'NOME', 'SALDO']])
