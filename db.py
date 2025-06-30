import fdb
from datetime import date, timedelta


def db_resulth() -> tuple:
    with fdb.connect(
            host='resulthserv', database='c:\\ResWinCS\\Banco\\RESULTH.FB',
            user='sysdba', password='masterkey') as conn:

        cursor = conn.cursor()
        un_year = timedelta(days=365)
        two_years = timedelta(days=730)
        today = date.today()

        # Consulta ao total faturado em 12 meses
        cursor.execute(
            f"""SELECT SUM(p.TOTALPEDIDO)
            FROM PEDIDOC p
            WHERE p.DATAFATURA BETWEEN '{today - un_year}' AND '{today}'
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
