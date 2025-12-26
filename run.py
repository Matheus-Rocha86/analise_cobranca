import pandas as pd
from regra_de_negocio import RegraDeNegocio
from pathlib import Path


def run():
    # DADOS INICIAIS
    CAMINHO_AQRUIVO_ESTOQUE_INICIAL = Path("estoque_inicial_oleo_2025.csv")
    CAMINHO_AQRUIVO_ESTOQUE_FINAL = Path("estoque_final_oleo_2025.csv")
    start_date = '2025-01-01'

    # INSTACIA OBJETOS
    regra_de_negocio = RegraDeNegocio(
        data_inicial=start_date,
        caminho_tabela_estoque1=CAMINHO_AQRUIVO_ESTOQUE_INICIAL,
        caminho_tabela_estoque2=CAMINHO_AQRUIVO_ESTOQUE_FINAL
    )

    # CÁLCULO DO GIRO DE ESTOQUE
    giro_estoque = regra_de_negocio.calcular_giro_estoque()

    # CÁLCULO DO GIRO DE ESTOQUE POR PRODUTO
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    giro_estoque_por_produto = regra_de_negocio.calcular_giro_estoque_por_produto()
    estoque = regra_de_negocio.obter_cobertura_estoque()
    print(estoque)


if __name__ == "__main__":

    run()
