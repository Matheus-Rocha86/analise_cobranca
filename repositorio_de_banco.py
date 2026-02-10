from typing import Any, Tuple, List
from interface_banco import IRepositorioBancoAbstrato, RepositorioQuery
from conexao_banco_dados_resulth import Conexao
from queries import consultar_saldo_clientes_inadimplentes, conultar_saldo_contas_a_receber


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

    def consultar_clientes_faixa_atraso_dias(self, query) -> list:
        return self._executar_consulta(query)


class FaixaReceber:
    def __init__(self,
                 faixa_1_a_30_dias: bool = False,
                 faixa_31_a_60_dias: bool = False,
                 faixa_61_a_90_dias: bool = False,
                 faixa_91_a_120_dias: bool = False,
                 faixa_120_acima_dias: bool = False,
                 faixa_debito_perdido: bool = False) -> None:

        faixas = [
            faixa_1_a_30_dias,
            faixa_31_a_60_dias,
            faixa_61_a_90_dias,
            faixa_91_a_120_dias,
            faixa_120_acima_dias,
            faixa_debito_perdido
        ]
        # Conta quantos valores True existem
        total_true = sum(1 for faixa in faixas if faixa)

        # Validação: deve ter exatamente um True ou todos False
        if total_true > 1:
            raise ValueError("Apenas uma faixa pode ser marcada como True")
        self.faixa_1_a_30_dias = faixa_1_a_30_dias
        self.faixa_31_a_60_dias = faixa_31_a_60_dias
        self.faixa_61_a_90_dias = faixa_61_a_90_dias
        self.faixa_91_a_120_dias = faixa_91_a_120_dias
        self.faixa_120_acima_dias = faixa_120_acima_dias
        self.faixa_debito_perdido = faixa_debito_perdido

    def _selecionar_faixa_dias(self):
        faixas = {
            "faixa_1_a_30_dias": self.faixa_1_a_30_dias,
            "faixa_31_a_60_dias": self.faixa_31_a_60_dias,
            "faixa_61_a_90_dias": self.faixa_61_a_90_dias,
            "faixa_91_a_120_dias": self.faixa_91_a_120_dias,
            "faixa_120_acima_dias": self.faixa_120_acima_dias,
            "total_debito_perdido": self.faixa_debito_perdido
        }
        for faixa, valor in faixas.items():
            if valor:
                return faixa
        return 0

    def acessar_faixa_clientes_em_atraso(self):
        faixa = self._selecionar_faixa_dias()
        dicionario_faixa_comando = {
            "faixa_1_a_30_dias": "BETWEEN 1 AND 30",
            "faixa_31_a_60_dias": "BETWEEN 31 AND 60",
            "faixa_61_a_90_dias": "BETWEEN 61 AND 90",
            "faixa_91_a_120_dias": "BETWEEN 91 AND 120",
            "faixa_120_acima_dias": "> 120",
            "total_debito_perdido": "BETWEEN 1 AND 365"
        }
        if "total_debito_perdido" not in faixa:
            cliente_ativo = 'S'
            comando_selecionado = (cliente_ativo, dicionario_faixa_comando[faixa])
            return self._query_select_clientes_faixa_atraso_dias(comando_selecionado)
        cliente_ativo = 'N'
        comando_selecionado = (cliente_ativo, dicionario_faixa_comando[faixa])
        return self._query_select_clientes_faixa_atraso_dias(comando_selecionado)

    def _query_select_clientes_faixa_atraso_dias(self, comandos: Tuple) -> str:
        query = f"""
                --Clientes devedores agrupados por faixa de PMR
                WITH pmr AS (
                    SELECT
                        d.CODCLIENTE,
                        c.NOME,
                        ROUND(d.VALORDOCTO - d.VALORPAGO - COALESCE(d.VALORDESC, 0), 2) AS SALDO,
                        DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIAS_ATRASO
                    FROM DOCUREC d
                    INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                    WHERE d.SITUACAO NOT IN (2, 4)  -- Forma alternativa
                        AND d.TIPODOCTO <> 'CO'
                        AND c.ATIVO = '{comandos[0]}'  -- 'S' ou 'N'
                        AND DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) > 0
                )
                SELECT
                    CODCLIENTE,
                    NOME,
                    SUM(SALDO) AS SALDO_TOTAL,
                    ROUND(SUM(SALDO * DIAS_ATRASO) / NULLIF(SUM(SALDO), 0), 0) AS PRAZO_MEDIO_RECEBIMENTO
                FROM pmr
                GROUP BY CODCLIENTE, NOME
                HAVING SUM(SALDO) > 0  -- Opcional: exclui clientes com saldo zero/negativo
                    AND ROUND(SUM(SALDO * DIAS_ATRASO) / NULLIF(SUM(SALDO), 0), 0) {comandos[1]}  -- Condição de faixa de atraso
                ORDER BY SALDO_TOTAL DESC;
            """
        return query
