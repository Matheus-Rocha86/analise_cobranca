import pandas as pd
from pathlib import Path
from resuthquery import ResulthDatabase


class AnaliseEstoqueVenda(ResulthDatabase):
    def __init__(self,
                 caminho: Path,
                 start_date: str,
                 retornar_df_cmv=False) -> None:
        self.caminho = caminho
        self.start_date = start_date
        self.retornar_df_cmv = retornar_df_cmv

    def processar_dados(self):
        df = pd.read_csv(self.caminho,
                         header=None,
                         sep=';',
                         names=["COD", "QUANTIDADE", "VALOR", "TOTAL"])
        return df

    def retornar_dados(self):
        df = self.processar_dados()
        df["TOTAL"] = df["TOTAL"].str.replace(',', '.')
        df["TOTAL"] = pd.to_numeric(df["TOTAL"], errors='coerce')
        df = df.dropna(subset=['TOTAL'])
        return df

    def retornar_serie(self):
        df = self.retornar_dados()
        return df["TOTAL"]

    def totalizar_estoque(self):
        return self.retornar_serie().sum()

    def retornar_custo_mercadoria_vendida(self):
        # Instancia objeto que trabalho no banco de dados
        dados = ResulthDatabase(self.start_date)

        # Cria uma variável com os dados relativo ao custo das mercadoria vendidas
        dados_custo_mercadoria_vendida = dados.query_custo_mercadoria_vendida()

        # Cria um dataframe do custo das mercadorias vendidas
        df = pd.DataFrame(dados_custo_mercadoria_vendida,
                          columns=["COD", "QUANTIDADE", "VALOR", "TOTAL"])
        if self.retornar_df_cmv:
            df_novo = df.drop(["QUANTIDADE", "VALOR"], axis=1)
            return df_novo
        # Retorna o somatório do custo das vendas
        return df["TOTAL"].sum()


if __name__ == "__main__":
    def main():
        # DADOS INICIAIS
        CAMINHO_AQRUIVO_ESTOQUE_INICIAL = Path("estoque_inicial_2025.csv")
        CAMINHO_AQRUIVO_ESTOQUE_FINAL = Path("estoque_final_2025.csv")
        start_date = '2025-01-01'

        # INSTACIA OBJETOS
        dados_estoque_inicial = AnaliseEstoqueVenda(
            CAMINHO_AQRUIVO_ESTOQUE_INICIAL, start_date)
        dados_estoque_final = AnaliseEstoqueVenda(
            CAMINHO_AQRUIVO_ESTOQUE_FINAL, start_date)

        # CÁLCULO DO ESTOQUE INICIAL, FINAL E MÉDIO
        total_estoque_inicial = dados_estoque_inicial.totalizar_estoque()
        total_estoque_final = dados_estoque_final.totalizar_estoque()
        estoque_medio = (total_estoque_inicial + total_estoque_final) / 2

        # CUSTO DA MERCADORIA VENDIDA
        dados_custo_mercadoria_vendida = dados_estoque_inicial.retornar_custo_mercadoria_vendida()

        ##########################################
        print("=============================================")
        print(f"Custo da mercadoria vendida -> {dados_custo_mercadoria_vendida:2.0f}")
        print(f"Total do estoque inicial -> {total_estoque_inicial:2.0f}")
        print(f"Total do estoque final -> {total_estoque_final:2.0f}")
        print(f"Estoque médio -> {estoque_medio:2.0f}")
        print("=============================================")
        print(f"Giro de estoque -> {dados_custo_mercadoria_vendida / estoque_medio:2.0f}\n")

    def join():
        # DADOS INICIAIS
        CAMINHO_AQRUIVO_ESTOQUE_INICIAL = Path("estoque_inicial_2025.csv")
        CAMINHO_AQRUIVO_ESTOQUE_FINAL = Path("estoque_final_2025.csv")
        start_date = '2025-01-01'

        # INSTACIA OBJETOS
        dados_estoque_inicial = AnaliseEstoqueVenda(
            CAMINHO_AQRUIVO_ESTOQUE_INICIAL, start_date, retornar_df_cmv=True)
        dados_estoque_final = AnaliseEstoqueVenda(
            CAMINHO_AQRUIVO_ESTOQUE_FINAL, start_date)
        
        df_estoque_inicial = dados_estoque_inicial.processar_dados()
        df_estoque_final = dados_estoque_final.processar_dados()
        df_cmv = dados_estoque_inicial.retornar_custo_mercadoria_vendida()
        print(df_cmv)
        print(df_estoque_inicial)
        print(df_estoque_final)

    main()