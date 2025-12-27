import pandas as pd
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from regra_de_negocio import RegraDeNegocio
from repositorio_de_banco import RepositorioDeBanco
from repositorio_tabelas_estoques import RepositorioTabelasEstoques
from pathlib import Path


def criar_regra_de_negocio(
        data_inicial: str,
        caminho_estoque_inicial: Path,
        caminho_estoque_final: Path,
        conexao_banco: ConexaoBancoDadosResulth) -> RegraDeNegocio:

    repositorio_banco = RepositorioDeBanco(
        data_inicial=data_inicial,
        conexao=conexao_banco
    )

    repositorio_estoque_inicial = RepositorioTabelasEstoques(
        caminho_tabela_estoque=caminho_estoque_inicial
    )

    repositorio_estoque_final = RepositorioTabelasEstoques(
        caminho_tabela_estoque=caminho_estoque_final
    )

    return RegraDeNegocio(
        data_inicial=data_inicial,
        repositorio_banco=repositorio_banco,
        repositorio_estoque1=repositorio_estoque_inicial,
        repositorio_estoque2=repositorio_estoque_final
    )


def run():
    # Configurações iniciais
    CAMINHO_AQRUIVO_ESTOQUE_INICIAL = Path("estoque_inicial_oleo_2025.csv")
    CAMINHO_AQRUIVO_ESTOQUE_FINAL = Path("estoque_final_oleo_2025.csv")
    start_date = '2025-01-01'
    conexao_banco = ConexaoBancoDadosResulth()

    # INSTACIA OBJETOS
    regra_de_negocio = criar_regra_de_negocio(
        data_inicial=start_date,
        caminho_estoque_inicial=CAMINHO_AQRUIVO_ESTOQUE_INICIAL,
        caminho_estoque_final=CAMINHO_AQRUIVO_ESTOQUE_FINAL,
        conexao_banco=conexao_banco
    )

    # Executar cálculos:
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    giro_estoque = regra_de_negocio.calcular_giro_estoque()

    giro_estoque_por_produto = regra_de_negocio.calcular_giro_estoque_por_produto()

    estoque = regra_de_negocio.obter_cobertura_estoque()

    return {
        "giro_estoque": giro_estoque,
        "giro_por_produto": giro_estoque_por_produto,
        "cobertura": estoque
    }


if __name__ == "__main__":

    resultados = run()
