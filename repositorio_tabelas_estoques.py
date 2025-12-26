import pandas as pd
from pyparsing import Path


class RepositorioTabelasEstoques:
    """
    Repositório para manipulação de tabelas de estoques
    """
    def __init__(self, caminho_tabela_estoque: Path) -> None:
        self.caminho_tabela_estoque = caminho_tabela_estoque

    def carregar_tabela_estoque(self) -> pd.DataFrame:
        df_estoque = pd.read_csv(
            self.caminho_tabela_estoque,
            header=None,
            sep=';',
            names=["COD", "QUANTIDADE", "VALOR", "TOTAL"]
        )
        return df_estoque

    def retornar_serie_total_estoque(self) -> pd.Series:
        df_estoque = self.carregar_tabela_estoque()
        df_estoque["TOTAL"] = df_estoque["TOTAL"].str.replace(',', '.')
        df_estoque["TOTAL"] = pd.to_numeric(
            df_estoque["TOTAL"], errors='coerce')
        df_estoque = df_estoque.dropna(subset=['TOTAL'])
        return df_estoque["TOTAL"]

    def retornar_dataframe_estoque(self) -> pd.DataFrame:
        df_estoque = self.carregar_tabela_estoque()
        df_estoque["TOTAL"] = df_estoque["TOTAL"].str.replace(',', '.')
        df_estoque["TOTAL"] = pd.to_numeric(
            df_estoque["TOTAL"], errors='coerce')
        df_estoque = df_estoque.dropna(subset=['TOTAL'])
        return df_estoque
