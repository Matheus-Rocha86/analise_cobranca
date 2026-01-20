def consultar_ticket_medio() -> str:
    return """
            SELECT
                EXTRACT(MONTH FROM p.DATAFATURA) AS mes,
                round(sum(p.TOTALPEDIDO), 0) AS total_mes,
                COUNT(*) AS qtde_pv,
                round(round(sum(p.TOTALPEDIDO), 0) / COUNT(*), 0) AS ticket_medio
            FROM PEDIDOC p
            WHERE
	            p.DATAFATURA
		            BETWEEN CAST(? AS TIMESTAMP) AND CAST(? AS TIMESTAMP)
                AND p.TIPOPEDIDO = 55
                AND p.TIPODAV <> 'O'
                AND p.FATURADO = 'S'
            GROUP BY EXTRACT(MONTH FROM p.DATAFATURA)
            ORDER BY mes;
            """
