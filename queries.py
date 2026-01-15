def conultar_saldo_contas_a_receber():
    return """
            WITH saldos_clientes AS (
                SELECT
                    d.CODCLIENTE,
                    c.NOME,
                    CASE
                        WHEN d.SITUACAO = 3 THEN (d.VALORDOCTO - d.VALORPAGO)
                        WHEN d.SITUACAO = 1 THEN d.VALORDOCTO
                        ELSE 0
                    END AS valor,
                    d.SITUACAO
                FROM DOCUREC d
                INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                WHERE d.TIPODOCTO <> 'CO'
                    AND c.ATIVO = 'S'
                    AND d.DT_VENCIMENTO BETWEEN ? AND ?
                    AND d.SITUACAO IN (1, 3)
            ),
            temp AS (
                SELECT
                    CODCLIENTE,
                    NOME,
                    SUM(valor) AS saldo_total
                FROM saldos_clientes
                GROUP BY CODCLIENTE, NOME
            )
            SELECT
                SUM(saldo_total) AS saldo_contas_a_receber
            FROM temp
        """


def consultar_saldo_clientes_inadimplentes():
    return """
            WITH resultado_final AS (
            SELECT
                CODCLIENTE,
                NOME,
                COUNT(*) AS QTD_DOCUMENTOS,
                SUM(SALDO) AS SALDO_TOTAL,
                -- Cálculo correto do PMR (média ponderada dos dias em atraso)
                CAST(SUM(SALDO * DIAS_ATRASO) / SUM(SALDO) AS INT) AS Atraso_medio_dias
            FROM (
                SELECT
                    d.CODCLIENTE,
                    c.NOME,
                    d.VALORDOCTO,
                    d.VALORPAGO,
                    d.VALORDESC,
                    ROUND(d.VALORDOCTO - d.VALORPAGO, 2) AS SALDO,
                    DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIAS_ATRASO
                FROM DOCUREC d
                INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
                WHERE d.DT_VENCIMENTO BETWEEN (CURRENT_TIMESTAMP - 180) AND (CURRENT_TIMESTAMP - 1)
                    AND d.TIPODOCTO <> 'CO'
                    AND c.ATIVO = 'S'
                    AND VALORDOCTO - VALORPAGO > 0.01
                    AND VALORDESC = 0
                    --AND c.PESSOA_FJ = 'F'
            ) subquery
            GROUP BY CODCLIENTE, NOME
            -- Filtra por prazo médio de recebimento (PMR)
            HAVING SUM(SALDO * DIAS_ATRASO) / SUM(SALDO) BETWEEN 120 AND 180 -- atraso em dias
            ORDER BY Atraso_medio_dias DESC
            )
            SELECT SUM(SALDO_TOTAL) AS SALDO_TOTAL_GERAL FROM resultado_final;
            """
