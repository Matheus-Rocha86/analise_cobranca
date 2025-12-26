import pandas as pd
import numpy as np
from pyparsing import Path
from repositorio_de_banco import RepositorioDeBanco
from repositorio_tabelas_estoques import RepositorioTabelasEstoques


class RegraDeNegocio:
    """
    Calcula as regras de negócio para o giro de estoque.
    """
    def __init__(self,
                 data_inicial: str,
                 caminho_tabela_estoque1: Path,
                 caminho_tabela_estoque2: Path,
                 consulta_banco: RepositorioDeBanco = None,
                 consulta_tabelas_estoques: RepositorioTabelasEstoques = None) -> None:
        self.data_inicial = data_inicial
        self.caminho_tabela_estoque1 = caminho_tabela_estoque1
        self.caminho_tabela_estoque2 = caminho_tabela_estoque2
        self.__consulta_banco = consulta_banco or RepositorioDeBanco(data_inicial)
        self.__consulta_tabelas_estoques1 = consulta_tabelas_estoques or RepositorioTabelasEstoques(self.caminho_tabela_estoque1)
        self.__consulta_tabelas_estoques2 = consulta_tabelas_estoques or RepositorioTabelasEstoques(self.caminho_tabela_estoque2)

    def calcular_cmv(self):
        dados_cmv = self.__consulta_banco.buscar_dados()
        # Cria um dataframe do custo das mercadorias vendidas
        df = pd.DataFrame(dados_cmv,
                          columns=["COD", "DESCRICAO", "VALOR", "TOTAL"])
        return df["TOTAL"].sum()

    def calcular_estoque_total(self):
        """Calcula e retorna ambos os estoques totais."""
        estoque_inicial = self.__consulta_tabelas_estoques1.retornar_serie_total_estoque().sum()
        estoque_final = self.__consulta_tabelas_estoques2.retornar_serie_total_estoque().sum()
        return {"estoque_inicial": estoque_inicial, "estoque_final": estoque_final}

    def calcular_giro_estoque(self):
        cmv = self.calcular_cmv()
        estoque_total = self.calcular_estoque_total()
        estoque_medio = (estoque_total["estoque_inicial"] + estoque_total["estoque_final"]) / 2
        try:
            giro_estoque = cmv / estoque_medio
        except ZeroDivisionError:
            print("Erro: Divisão por zero ao calcular o giro de estoque.")
            giro_estoque = 0
        return giro_estoque

    def calcular_giro_estoque_por_produto(self):
        dados_cmv = self.__consulta_banco.buscar_dados()
        df_cmv = pd.DataFrame(dados_cmv,
                              columns=["COD", "QUANTIDADE_VENDIDA", "CUSTO_UNITARIO", "CUSTO_TOTAL"])
        # Exclui colunas desnecessárias
        df_cmv.drop(columns=["QUANTIDADE_VENDIDA", "CUSTO_UNITARIO"], axis=1, inplace=True)
        # Converte a coluna COD para int64
        df_cmv["COD"] = df_cmv["COD"].astype("int64")
        # Estoque inicial e final por produto
        df_estoque_inicial = self.__consulta_tabelas_estoques1.retornar_dataframe_estoque()
        df_estoque_final = self.__consulta_tabelas_estoques2.retornar_dataframe_estoque()
        # Renomeia colunas para evitar conflitos na mesclagem
        df_estoque_inicial = df_estoque_inicial.rename(columns={"TOTAL": "TOTAL_INICIAL"})
        df_estoque_final = df_estoque_final.rename(columns={"TOTAL": "TOTAL_FINAL"})
        # Excluir colunas desnecessárias
        df_estoque_inicial.drop(columns=["QUANTIDADE", "VALOR"], axis=1, inplace=True)
        df_estoque_final.drop(columns=["QUANTIDADE", "VALOR"], axis=1, inplace=True)
        # Mescla os dataframes de estoque inicial e final
        df_estoque = pd.merge(df_estoque_inicial, df_estoque_final,
                              on="COD", how="outer").fillna(0)
        # Mescla o dataframe de CMV com o dataframe de estoque
        df_estoque_custo_total = pd.merge(df_cmv, df_estoque,
                          on="COD", how="outer").fillna(0)
        # Calcula o giro de estoque por produto
        giro_estoque_produto = []
        for indice, linha in df_estoque_custo_total.iterrows():
            estoque_medio = (linha["TOTAL_INICIAL"] + linha["TOTAL_FINAL"]) / 2
            if pd.isna(estoque_medio) or pd.isna(linha["CUSTO_TOTAL"]):
                giro_estoque_produto.append(np.nan)
            elif estoque_medio == 0:
                # Divisão por zero - decide o que fazer
                giro_estoque_produto.append(np.inf)  # ou 0, ou np.nan
            else:
                # Só divide se for seguro
                resultado = linha["CUSTO_TOTAL"] / estoque_medio
                giro_estoque_produto.append(resultado)
        df_estoque_custo_total["GIRO_ESTOQUE"] = giro_estoque_produto
        # exlui linhas em que o giro de estoque é infinito
        df_estoque_custo_total = df_estoque_custo_total.replace([np.inf, -np.inf], np.nan)
        df_estoque_custo_total = df_estoque_custo_total.dropna(subset=['GIRO_ESTOQUE'])
        return df_estoque_custo_total.sort_values(by="GIRO_ESTOQUE", ascending=False)

    def obter_estatisticas_giro_estoque(self):
        df_giro_estoque = self.calcular_giro_estoque_por_produto()
        return df_giro_estoque["GIRO_ESTOQUE"].describe()

    def obter_cobertura_estoque(self):

        # Dados obtidos
        dados = self.calcular_giro_estoque_por_produto()

        dados_filtrados = dados[dados["TOTAL_FINAL"] > 0]

        dados_filtrados = dados_filtrados[dados_filtrados["GIRO_ESTOQUE"] != 0]

        cobertura_de_estoque = []
        for indice, linha in dados_filtrados.iterrows():
            cobertura_de_estoque.append(365 / linha["GIRO_ESTOQUE"])
        dados_filtrados["COBERTURA"] = cobertura_de_estoque
        dados_filtrados.drop(columns=["CUSTO_TOTAL", "TOTAL_INICIAL", "TOTAL_FINAL"], inplace=True)
        return dados_filtrados.to_string(index=False)