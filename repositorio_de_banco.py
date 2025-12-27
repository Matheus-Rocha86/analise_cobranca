from conexao_banco_dados_resulth import ConexaoBancoDadosResulth


class RepositorioDeBanco(ConexaoBancoDadosResulth):
    """
    Classe que expressa as consultas em SQL ao banco de dados.
    """
    def __init__(self,
                 data_inicial: str,
                 conexao: ConexaoBancoDadosResulth) -> None:
        self.data_inicial = data_inicial
        self.__conexao = conexao

    def select_cmv(self):
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

    def buscar_dados(self):
        if self.__conexao.get_connection():
            with self.__conexao.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.select_cmv(), (self.data_inicial,))
                return cursor.fetchall()
        return None
