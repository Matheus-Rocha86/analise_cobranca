from typing import Any, Tuple, List
from interface_banco import IRepositorioBancoAbstrato, RepositorioQuery
from conexao_banco_dados_resulth import Conexao
from queries import consultar_saldo_clientes_inadimplentes, conultar_saldo_contas_a_receber


class AtributosComuns:
    def __init__(self,
                 data_inicial: str,
                 data_final: str,
                 conexao_banco: Conexao) -> None:
        self.data_inicial = data_inicial
        self.data_final = data_final
        self.conexao_banco = conexao_banco


class RepositorioDeBanco(IRepositorioBancoAbstrato):
    """
    Classe que expressa as consultas em SQL ao banco de dados.
    """
    def __init__(self,
                 data_inicial: str,
                 conexao: Conexao) -> None:
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
                 data_final: str,
                 conexao: Conexao,
                 saldo_total=True) -> None:
        super().__init__(
            data_inicial=data_inicial,
            conexao=conexao)
        self.data_final = data_final
        self.saldo_total = saldo_total

    def _construir_query_select_base(self, campos: str) -> str:
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

    def _query_select_saldo_vencido(self) -> str:
        return """
                SELECT
                    d.CODCLIENTE,
                    c.NOME,
                    d.DT_VENCIMENTO,
                    d.VALORDOCTO,
                    (d.VALORDOCTO - d.VALORPAGO) AS SALDO,
                    DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIFF_DATA
                FROM DOCUREC d
                INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                WHERE d.DT_VENCIMENTO BETWEEN '{self.end_date - self.large_day}' AND '{self.end_date - self.one_day}'
                    AND d.TIPODOCTO <> 'CO'
                    AND c.ATIVO = 'S'
                    AND (d.VALORDOCTO - d.VALORPAGO) > 0.05
                    AND d.VALORPAGO + d.VALORDESC <> d.VALORDOCTO
                    AND d.VALORDOCTO > 1
                ORDER BY d.DT_VENCIMENTO
                """

    def _query_select_saldo_vencido_clientes(self, campos: Tuple) -> str:
        return f"""
                {campos[0]}
                SELECT
                    CODCLIENTE,
                    NOME,
                    COUNT(*) AS QTD_DOCUMENTOS,
                    SUM(SALDO) AS SALDO_TOTAL,
                -- Cálculo correto do PMR (média ponderada dos dias em atraso)
                CAST(SUM(SALDO * DIAS_ATRASO) / NULLIF(SUM(SALDO), 0) AS INT) AS Atraso_medio_dias
                FROM (
                    SELECT
                        d.CODCLIENTE,
                        c.NOME,
                        d.VALORDOCTO,
                        d.VALORPAGO,
                        d.VALORDESC,
                        ROUND(d.VALORDOCTO - d.VALORPAGO - COALESCE(d.VALORDESC, 0), 2) AS SALDO,
                        -- Calcula dias de atraso apenas para documentos vencidos
                        CASE
                            WHEN d.DT_VENCIMENTO < CURRENT_TIMESTAMP
                            THEN DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP)
                            ELSE 0
                        END AS DIAS_ATRASO
                    FROM DOCUREC d
                    INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                    WHERE d.DT_VENCIMENTO < CURRENT_TIMESTAMP
                        AND d.DT_VENCIMENTO >= DATEADD(DAY, -180, CAST(CURRENT_TIMESTAMP AS DATE))  -- Últimos 180 dias
                        AND d.TIPODOCTO <> 'CO'
                        AND c.ATIVO = 'S'
                        AND (d.VALORDOCTO - d.VALORPAGO - COALESCE(d.VALORDESC, 0)) > 0.01
                        AND (d.VALORDESC IS NULL OR d.VALORDESC = 0)
                ) subquery
                WHERE DIAS_ATRASO > 0  -- Apenas documentos em atraso
                GROUP BY CODCLIENTE, NOME
                -- Filtra por prazo médio de recebimento (PMR)
                HAVING SUM(SALDO) > 0
                    AND CAST(SUM(SALDO * DIAS_ATRASO) / SUM(SALDO) AS INT) BETWEEN 120 AND 180
                ORDER BY Atraso_medio_dias DESC, SALDO_TOTAL DESC
                {campos[1]}
                {campos[2]}
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

    def processar_dados_saldo_vencido(self) -> List[Tuple]:
        query = self._query_select_saldo_vencido()
        return self._executar_consulta(query)

    def processar_dados_total_vendido_clientes(self) -> List[Tuple]:
        if self.saldo_total:
            campo1 = "WITH resultado_final AS ("
            campo2 = ")"
            campo3 = "SELECT SUM(SALDO_TOTAL) AS SALDO_TOTAL_GERAL FROM resultado_final;"
            campos = (campo1, campo2, campo3)
        else:
            campos = ("", "", "")
        query = self._query_select_saldo_vencido_clientes(campos)
        return self._executar_consulta(query)


class ConsultaExecutor(RepositorioQuery):
    def __init__(self,
                 data_inicial: str,
                 data_final: str,
                 conexao_banco: Conexao) -> None:
        self.data_inicial = data_inicial
        self.data_final = data_final
        self._conexao_banco = conexao_banco

    def _executar_consulta(self, query: str) -> List[Tuple]:
        try:
            self._conexao_banco.get_connection()
        except Exception as e:
            raise ValueError("Erro ao conectar com o banco.") from e
        else:
            with self._conexao_banco.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query,
                               (self.data_inicial, self.data_final))
                return cursor.fetchall()

    def conultar_saldo_contas_a_receber(self) -> list:
        query = conultar_saldo_contas_a_receber()
        return self._executar_consulta(query)

    def consultar_saldo_clientes_inadimplentes(self) -> list:
        query = consultar_saldo_clientes_inadimplentes()
        return self._executar_consulta(query)
