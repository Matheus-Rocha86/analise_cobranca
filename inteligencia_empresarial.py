import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import date
from resuthquery import ResulthDatabase
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score


class RelatoriosDeVendas(ResulthDatabase):
    """
    A classe reuni informações sobre o movimento
    de venda nas suas várias formas: quantidade e
    fatuamento. Além de gerar gráficos de regressão linear
    e da média móvel.

    :param self:
    """
    def __init__(self,
                 quantidade_total_diaria: bool,
                 agrupar_dados_faturamento=False,
                 grafico_media_movel=False,
                 grafico_regressao_linear=False,
                 **kwargs: str) -> None:
        super().__init__(
            quantidade_total_diaria=quantidade_total_diaria,
            **kwargs)
        self.quantidade_total_diaria = quantidade_total_diaria
        self.agrupar_dados_faturamento = agrupar_dados_faturamento
        self.grafico_media_movel = grafico_media_movel
        self.grafico_regressao_linear = grafico_regressao_linear

    def gerar_dados_das_vendas(self):
        dados = self.query_pv()
        if self.quantidade_total_diaria:
            df_dados = pd.DataFrame(dados, columns=["DATA", "TOTAL"])
            return df_dados["TOTAL"]  # Quantidade total diária
        else:
            # Faturamento total diário não agrupado
            df_dados = pd.DataFrame(dados, columns=["DATA", "COD", "TOTAL"])
            df_dados.drop("COD", axis=1, inplace=True)
            if self.agrupar_dados_faturamento:
                # Faturamento total diária agrupado
                df_dados_grouped = df_dados.groupby("DATA", as_index=False)["TOTAL"].sum()
                return df_dados_grouped["TOTAL"]
            return df_dados

    def calcular_regressao_linear(self):
        """Calcula a regressão linear para uma série de vendas"""
        # Gerador de dias
        vendas = self.gerar_dados_das_vendas()
        dias = np.array([x for x in range(len(vendas))])
        # Regressão linear
        x = dias.reshape(-1, 1)
        modelo = LinearRegression()
        modelo.fit(x, vendas)
        tendencia = modelo.predict(x)
        # Calcular R²
        r2 = r2_score(vendas, tendencia)
        return dias, tendencia, modelo, r2

    def calcular_media_movel(self):
        # Para dados com mais flutuações
        vendas = self.gerar_dados_das_vendas()
        dias = np.array([x for x in range(len(vendas))])
        # Médias móveis com diferentes janelas
        #media_3_dias = np.convolve(vendas, np.ones(3)/3, mode='valid')
        #media_5_dias = np.convolve(vendas, np.ones(5)/5, mode='valid')
        media_7_dias = np.convolve(vendas, np.ones(7)/7, mode='valid')
        # Ajustar índices para as médias móveis
        #dias_3 = dias[1:-1]
        #dias_5 = dias[2:-2]
        dias_7 = dias[3:-3]
        return dias, media_7_dias, dias_7

    def plotar_grafico(self):
        """Cria gráfico da regressão linear ou da média móvel
        a depender da variável de controle **grafico_medial_movel**."""

        # Criar figura do gráfico
        plt.figure(figsize=(10, 6))
        # Parâmetros
        vendas = self.gerar_dados_das_vendas()
        if self.grafico_regressao_linear:
            dias, tendencia, modelo, r2 = self.calcular_regressao_linear()
            # Gráfico 1: Vendas x dias
            plt.plot(vendas.index, vendas, marker='o', linestyle='none',
                     label='Vendas Diárias', color='blue', markersize=4)
            # Gráfico 2: regressão linear
            plt.plot(dias, tendencia, '-', linewidth=2,
                     label=f'Linha de Tendência (R² = {r2:.3f})', color='red')
            # Configurações do gráfico
            plt.title('Regressão Linear - Vendas 2024')
            plt.xlabel('Dias')
            plt.ylabel('Contagem de pré-vendas faturadas')
            plt.legend()
            plt.grid(True)
            plt.show()
        elif self.grafico_media_movel:
            dias, media_7_dias, dias_7 = self.calcular_media_movel()
            # Gráfico 1: Vendas x dias
            plt.plot(vendas.index, vendas, label='Vendas diárias',
                     color='indigo', alpha=0.7)
            # Gráfico 2: regressão linear
            plt.plot(dias_7, media_7_dias, '-', linewidth=2.5,
                     label='Média Móvel (7 dias)', color='orange')
            # Configurações do gráfico
            plt.title('Regressão Linear - Vendas 2024')
            plt.xlabel('Dias')
            plt.ylabel('Contagem de pré-vendas faturadas')
            plt.legend()
            plt.grid(True)
            plt.show()

    def ticket_medio(self):
        if not self.quantidade_total_diaria:
            return self.gerar_dados_das_vendas()["TOTAL"].mean()
        return None

    def media_quantidade(self):
        if self.quantidade_total_diaria:
            return self.gerar_dados_das_vendas().mean()
        return None

    def media_faturamento(self):
        if not self.quantidade_total_diaria:
            return self.gerar_dados_das_vendas().mean()
        return None

    def gerar_df_tabela_os(self):
        return pd.DataFrame(self.query_os(), columns=["DATA_FATURAMENTO",
                                                      "NUM_OS",
                                                      "VLR_SERVICOS",
                                                      "VLR_PECAS",
                                                      "VLR_TOTAL"])

    def total_da_os(self):
        df_tabela_os = self.gerar_df_tabela_os()
        total_da_os = df_tabela_os["VLR_TOTAL"].sum()
        return total_da_os

    def total_da_pv(self):
        total_pv = self.gerar_dados_das_vendas()
        return total_pv.sum()

    def percentual_vendas_os(self):
        total_pv = self.total_da_pv()
        total_os = self.total_da_os()
        percentual = (total_os / total_pv) * 100
        return percentual


if __name__ == "__main__":
    start_date = "2025-12-04"
    end_date = "2025-12-04"
    datas = {
        'start_date': start_date,
        'end_date': end_date
    }
    dados = RelatoriosDeVendas(**datas,
                               quantidade_total_diaria=False,
                               agrupar_dados_faturamento=True)
    print("======2025======")
    print(f"Total da O.S -> {dados.total_da_os()}")
    print(f"Total da P.V -> {dados.total_da_pv()}")
    print("------------------------")
    print(f"O percetual da O.S: {dados.percentual_vendas_os():.0f}%")
    print()

    df = pd.DataFrame(dados.query_os(), columns=["DATA", "Nro O.S", "SERVICO", "PECAS", "VALOR O.S"])
    df_organizado = df.sort_values(by='Nro O.S', ascending=True).reset_index()
    soma = df["VALOR O.S"]
    print(df_organizado)
