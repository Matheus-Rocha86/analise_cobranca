from datetime import date, timedelta
from typing import List, Tuple, Optional, Any
import fdb
import pandas as pd
from matplotlib import pyplot as plt
from salve import to_save_json 


class Database:
    """Classe base genérica para operações com banco de dados Firebird."""
    
    def __init__(self, host: str, database: str, user: str, password: str):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self._connection_params = {
            'host': self.host,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

    def _get_connection(self):
        """Retorna uma conexão com o banco de dados."""
        return fdb.connect(**self._connection_params)

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

    def close(self):
        """Fecha a conexão (se estiver aberta)."""
        if hasattr(self, '_conn') and self._conn:
            self._conn.close()


class ResulthDatabase(Database):
    """Classe especializada para operações no banco Resulth."""

    def get_financial_summary(self) -> Tuple[float, float, float]:
        """Obtém o resumo financeiro: faturamento, vencidos e a vencer."""
        today = date.today()
        one_year = timedelta(days=365)
        two_years = timedelta(days=730)

        # Total faturado em 12 meses
        total_faturamento = self.execute_scalar(
            f"""SELECT SUM(p.TOTALPEDIDO)
            FROM PEDIDOC p
            WHERE p.DATAFATURA BETWEEN '{today - one_year}' AND '{today}'
            AND p.TIPOPEDIDO = 55 AND FATURADO = 'S' AND p.TIPODAV <> 'O'
            """
        )

        # Saldo VENCIDO do Contas a Receber
        total_vencidos = self.execute_scalar(
            f"""SELECT SUM(d.VALORDOCTO) - SUM(d.VALORPAGO)
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '2025-02-01' AND '{today}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            """
        )

        # Saldo a VENCER do Contas a Receber
        total_a_vencer = self.execute_scalar(
            f"""SELECT SUM(d.VALORDOCTO) - SUM(d.VALORPAGO)
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '{today}' AND '{today + two_years}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            """
        )

        return total_faturamento or 0.0, total_vencidos or 0.0, total_a_vencer or 0.0

    def get_overdue_balances(self) -> List[Tuple]:
        """Obtém os saldos vencidos com detalhes."""
        today = date.today()
        one_day = timedelta(days=1)
        large_day = timedelta(days=180)

        return self.execute_query(
            f"""SELECT d.CODCLIENTE, c.NOME, d.DT_VENCIMENTO, d.VALORDOCTO,
                (d.VALORDOCTO - d.VALORPAGO) AS SALDO,
                DATEDIFF(DAY, d.DT_VENCIMENTO, CURRENT_TIMESTAMP) AS DIFF_DATA
            FROM DOCUREC d
            INNER JOIN CLIENTE c ON d.CODCLIENTE = c.CODCLIENTE
            WHERE d.DT_VENCIMENTO BETWEEN '{today - large_day}' AND '{today - one_day}'
            AND d.TIPODOCTO <> 'CO' AND c.ATIVO = 'S'
            AND (d.VALORDOCTO - d.VALORPAGO) > 0.05 AND d.VALORPAGO + d.VALORDESC <> d.VALORDOCTO AND d.VALORDOCTO > 1
            ORDER BY d.DT_VENCIMENTO
            """
        )


if __name__ == "__main__":
    # Configuração do banco
    resulth_db = ResulthDatabase(
        host='resulthserv',
        database='c:\\ResWinCS\\Banco\\RESULTH.FB',
        user='sysdba',
        password='masterkey'
    )

    # Obtém resumo financeiro
    faturamento, vencidos, a_vencer = resulth_db.get_financial_summary()
    #print(f"Faturamento: R$ {faturamento:,.2f}")
    #print(f"Vencidos: R$ {vencidos:,.2f}")
    #print(f"A vencer: R$ {a_vencer:,.2f}")

    # Criar o DataFrame no Pandas
    # Formatação numérica de duas casas decimais
    pd.options.display.float_format = '{:.2f}'.format
    df = pd.DataFrame(
        resulth_db.get_overdue_balances(),
        columns=['COD', 'NOME', 'DATA', 'VLRDOCTO', 'SALDO', 'DIFF_DATA']
    )

    # Realiza o produto da coluna SALDO com a coluna DIFF_DATA
    prod_saldo_diffdata = []
    for linha in df.itertuples():
        prod_saldo_diffdata.append(linha.SALDO * linha.DIFF_DATA)
    df['PRODUTO'] = prod_saldo_diffdata

    # Realiza o agrupamento dos clientes
    df_gruouped = df.groupby(['COD', 'NOME']).agg({
        'SALDO': 'sum',
        'PRODUTO': 'sum'
    }).reset_index()

    # Cálculo do PMR geral
    pmr_global = ((vencidos + a_vencer) / faturamento) * 365

    # Cálculo do PMR por cliente
    pmr_customers = []
    for row in df_gruouped.itertuples():
        pmr_customers.append(row.PRODUTO / row.SALDO)

    # Visualização dos dados
    df_gruouped['PMR'] = pmr_customers
    df_finally = df_gruouped[['COD', 'NOME', 'SALDO', 'PMR']]
    gp = df_finally.loc[df_finally['PMR'] > pmr_global].reset_index()
    # Imprimir
    #print(gp[['COD', 'NOME', 'SALDO']])
    # Visualização em gráficos
    # Quantidade de contas vencidas
    # <30 dias
    print(df_finally['PMR'].value_counts(bins=5))

    # Plots
    # Faixas de valores
    track_1 = df_finally.loc[(df_finally['PMR'] <= 30)].reset_index()  # Até 30 dias
    track_2 = df_finally.loc[(df_finally['PMR'] > 30) & (df_finally['PMR'] <= 60)].reset_index()  # De 30 até 60 dias
    track_3 = df_finally.loc[(df_finally['PMR'] > 60) & (df_finally['PMR'] <= 90)].reset_index()  # De 60 até 90 dias
    track_4 = df_finally.loc[(df_finally['PMR'] > 90) & (df_finally['PMR'] <= 120)].reset_index()  # De 90 até 120 dias
    track_5 = df_finally.loc[(df_finally['PMR'] > 120) & (df_finally['PMR'] <= 180)].reset_index()  # De 120 até 180 dias

    # Totais das faixas
    tot_track_1 = track_1['SALDO'].sum()
    tot_track_2 = round(track_2['SALDO'].sum(), ndigits=2)
    tot_track_3 = track_3['SALDO'].sum()
    tot_track_4 = track_4['SALDO'].sum()
    tot_track_5 = track_5['SALDO'].sum()

    categories = ['<30 dias', '30 a 60 dias', '60 a 90 dias', '90 a 120 dias', '120 dias>']
    data = [tot_track_1, tot_track_2, tot_track_3, tot_track_4, tot_track_5]
    barr = plt.bar(categories, data, color='skyblue')
    def adicionar_rotulos(bar_plot, data):
        for i, valor in enumerate(data):
            plt.text(i, valor, f'{valor:_}'.replace('.', ',').replace('_', '.'), ha='center', va='bottom')
    adicionar_rotulos(plt.bar(categories, data), data)
    plt.title('Total em atraso por tempo em dias')
    plt.ylabel('Total em atraso (R$)')
    plt.show()

    to_save_json(categories, data)
