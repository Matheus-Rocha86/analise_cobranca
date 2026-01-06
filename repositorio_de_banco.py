from typing import Any, Tuple, List
from interface_banco import IRepositorioBancoAbstrato
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth


class RepositorioDeBanco(IRepositorioBancoAbstrato):
    """
    Classe que expressa as consultas em SQL ao banco de dados.
    """
    def __init__(self,
                 data_inicial: str,
                 conexao: ConexaoBancoDadosResulth) -> None:
        self.data_inicial = data_inicial
        self._conexao = conexao

    def consultar_dados(self):
        return """
                WITH t AS (
                    SELECT
                        p2.CODPROD AS cod,
                        SUM(p2.QUANTIDADE) AS quantidade_total
                    FROM PEDIDOC p
                    INNER JOIN PEDIDOI p2
                        ON p.CODPEDIDO = p2.CODPEDIDO
                    INNER JOIN PRODUTO p3
                        ON p2.CODPROD = p3.CODPROD
                    INNER JOIN CURVAABC c
                        ON p2.CODPROD = c.CODPROD
                    WHERE p.DATAFATURA BETWEEN ? AND CAST(CURRENT_TIMESTAMP AS DATE)
                        AND p.TIPOPEDIDO = 55
                        AND p.TIPODAV <> 'O'
                        AND p.FATURADO = 'S'
                        AND p2.CODPROD <> '000321'
                        AND p3.TIPOPROD <> 'S'
                    GROUP BY p2.CODPROD
                )
                SELECT
                    t.cod,
                    t.quantidade_total,
                    (c.TOTCUSTO / NULLIF(t.quantidade_total, 0)) AS custo_unitario,
                    (t.quantidade_total * (c.TOTCUSTO / NULLIF(t.quantidade_total, 0))) AS custo_total
                FROM t
                INNER JOIN CURVAABC c
                    ON c.CODPROD = t.cod
                ORDER BY t.quantidade_total DESC;
                """

    def processar_dados(self):
        try:
            self._conexao.get_connection()
        except Exception as e:
            raise ValueError("Erro ao conectar com o banco.") from e
        else:
            with self._conexao.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.consultar_dados(), (self.data_inicial,))
                return cursor.fetchall()


class FaturamentoReceber(RepositorioDeBanco):
    def __init__(self,
                 data_inicial: str,
                 conexao: ConexaoBancoDadosResulth,
                 data_final: str) -> None:
        super().__init__(data_inicial, conexao)
        self.data_final = data_final

    def _construir_query_select_base(self, campos) -> str:
        return f"""
                SELECT DISTINCT
                    d.DT_EMISSAO,
                    {campos}
                FROM DOCUREC d
                WHERE
                    d.TIPODOCTO <> 'CO'
                    AND d.DT_EMISSAO BETWEEN ? AND ?
                GROUP BY d.DT_EMISSAO;
                """

    def _executar_consulta(self, query: str) -> List[Tuple]:
        try:
            self._conexao.get_connection()
        except Exception as e:
            raise ValueError("Erro ao conectar com o banco.") from e
        else:
            with self._conexao.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query,
                               (self.data_inicial, self.data_final))
                return cursor.fetchall()

    def query_select_faturamento_a_receber(self) -> str:
        return self._construir_query_select_base("SUM(d.VALORDOCTO) AS total")

    def query_select_faturamento_recebido(self) -> str:
        return self._construir_query_select_base("SUM(d.VALORPAGO) AS total_recebido")

    def processar_dados_faturamento_a_receber(self) -> List[Tuple]:
        query = self.query_select_faturamento_a_receber()
        return self._executar_consulta(query)

    def processar_dados_faturamento_recebido(self) -> List[Tuple]:
        query = self.query_select_faturamento_recebido()
        return self._executar_consulta(query)
