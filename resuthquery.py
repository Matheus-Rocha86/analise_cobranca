import fdb
from datetime import date, timedelta
from typing import List, Tuple, Optional, Any


class ResulthDatabase:
    """Classe especializada para operações no banco Resulth."""
    def __init__(self,
                 start_date=None,
                 end_date=None,
                 quantidade_total_diaria=False) -> None:
        self.start_date = start_date
        self.end_date = date.today() if end_date is None else end_date
        self.quantidade_total_diaria = quantidade_total_diaria
        self.one_year = timedelta(days=365)
        self.two_years = timedelta(days=730)
        self.large_day = timedelta(days=180)
        self.one_day = timedelta(days=1)
        self._connection_params = {
            'host': 'resulthserv',
            'database': 'c:\\ResWinCS\\Banco\\RESULTH.FB',
            'user': 'sysdba',
            'password': 'masterkey'
        }

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Executa uma consulta SQL e retorna os resultados."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def execute_scalar(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Executa uma consulta SQL e retorna um único valor (primeira coluna da primeira linha)."""
        results = self.execute_query(query, params)
        return results[0][0] if results else None

    def _get_connection(self):
        """Retorna uma conexão com o banco de dados."""
        return fdb.connect(**self._connection_params)

    def close(self):
        """Fecha a conexão (se estiver aberta)."""
        if hasattr(self, '_conn') and self._conn:
            self._conn.close()

    # ##################QUERIES##################

    def get_financial_summary(self) -> Tuple[float, float, float]:
        """Obtém o resumo financeiro: faturamento, vencidos e a vencer."""
        # Total faturado em 12 meses
        total_faturamento = self.execute_scalar(
            f"""SELECT SUM(p.TOTALPEDIDO)
            FROM PEDIDOC p
            WHERE p.DATAFATURA BETWEEN '{self.end_date - self.one_year}' AND '{self.end_date}'
            AND p.TIPOPEDIDO = 55 AND FATURADO = 'S' AND p.TIPODAV <> 'O'
            """
        )

        # Saldo VENCIDO do Contas a Receber
        total_vencidos = self.execute_scalar(
            f"""SELECT
                    SUM(d.VALORDOCTO) - SUM(d.VALORPAGO)
                FROM DOCUREC d
                INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                WHERE d.DT_VENCIMENTO BETWEEN '2025-02-01' AND '{self.end_date}'
                AND d.TIPODOCTO <> 'CO'
                AND c.ATIVO = 'S'
            """
        )

        # Saldo a VENCER do Contas a Receber
        total_a_vencer = self.execute_scalar(
            f"""SELECT SUM(d.VALORDOCTO) - SUM(d.VALORPAGO)
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '{self.end_date}' AND '{self.end_date + self.two_years}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            """
        )
        return total_faturamento or 0.0, total_vencidos or 0.0, total_a_vencer or 0.0

    def get_overdue_balances(self) -> List[Tuple]:
        """Obtém os saldos vencidos com detalhes."""
        return self.execute_query(
            f"""SELECT
                    d.CODCLIENTE,
                    c.NOME,
                    d.DT_VENCIMENTO,
                    d.VALORDOCTO,
                    ROUND(d.VALORDOCTO - d.VALORPAGO, 2) AS SALDO,
                    DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIFF_DATA
                FROM DOCUREC d
                INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                WHERE d.DT_VENCIMENTO BETWEEN '{self.end_date - self.large_day}' AND '{self.end_date - self.one_day}'
                AND d.TIPODOCTO <> 'CO'
                AND c.ATIVO = 'S'
                AND (d.VALORDOCTO - d.VALORPAGO) > 0.05
                AND VALORDESC = 0
                ORDER BY d.DT_VENCIMENTO
                """
        )

    def query_os(self):
        """Retorna dados da O.S"""
        return self.execute_query(
            f"""
            SELECT
                CAST(oo.DATAFATURAMENTO AS DATE) AS DATA_FATURAMENTO,
                oo.IDOS_OSCABECALHO,
                oo.VALORSERVICOPROPRIO,
                oo.VALORPECAS,
                (oo.VALORSERVICOPROPRIO + oo.VALORPECAS - (oo.VALORDESCONTOITENS + oo.VALORDESCONTO)) AS TOTAL_OS
            FROM OS_OSCABECALHO oo
            WHERE oo.DATAFATURAMENTO
                BETWEEN CAST('{self.start_date}' AS TIMESTAMP) AND CAST('{self.end_date}' AS TIMESTAMP)
            """)

    def query_pv(self):
        """Retorna dados contidos na pré-venda"""
        return self.execute_query(
            f"""
            SELECT
                CAST(p.DATAFATURA AS DATE) AS DATA_FATURAMENTO,
                {'COUNT(*) AS QUANTIDADE_PREVENDA' if self.quantidade_total_diaria else
                'p.CODPEDIDO,'
                'p.TOTALPEDIDO'}
            FROM PEDIDOC p
            WHERE p.DATAFATURA
            BETWEEN '{self.start_date}' AND '{self.end_date}'
                AND p.FATURADO = 'S'
                AND p.CODPEDIDO NOT LIKE '100%'
                AND p.CODPEDIDO NOT LIKE '101%'
                AND p.TIPODAV <> 'O'
            {'GROUP BY CAST(p.DATAFATURA AS DATE)' if self.quantidade_total_diaria else ''}
            """)

    def query_custo_mercadoria_vendida(self):
        return self.execute_query(
            f"""
            SELECT DISTINCT
            	p2.CODPROD,
                c.DESCRICAO,
                c.QUANTIDADE,
                c.TOTCUSTO
            FROM PEDIDOC p
            INNER JOIN PEDIDOI p2
            	ON p.CODPEDIDO = p2.CODPEDIDO
            INNER JOIN PRODUTO p3
            	ON p2.CODPROD = p3.CODPROD
            INNER JOIN CURVAABC c
            	ON p2.CODPROD = c.CODPROD
            WHERE p.DATAFATURA BETWEEN '{self.start_date}' AND CAST(CURRENT_TIMESTAMP AS DATE)
            	AND p.TIPOPEDIDO = 55
                AND p.TIPODAV <> 'O'
                AND p.FATURADO = 'S'
                AND p2.CODPROD <> '000321'
                AND p3.TIPOPROD <> 'S'
            ORDER BY c.QUANTIDADE DESC
            """)


if __name__ == "__main__":
    import pandas as pd
    start_date = "2025-01-01"
    end_date = "2025-12-01"

    teste = ResulthDatabase(start_date, end_date, quantidade_total_diaria=False)
    resultado = teste.query_pv()
    df_resultado = pd.DataFrame(resultado, columns=["DATA", "COD", "TOTAL"])

    print(df_resultado)