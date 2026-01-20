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
                    AND c.ATIVO = 'S'
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
                AND ROUND(SUM(SALDO * DIAS_ATRASO) / NULLIF(SUM(SALDO), 0), 0) > 120
            ORDER BY SALDO_TOTAL DESC;
            """
