from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from repositorio_de_banco import ConsultaExecutor
from inadimplencia import InadimplenciaSimples


def criar_regras_de_negocio(
        data_inicial: str,
        data_final: str,
        conexao: ConexaoBancoDadosResulth) -> InadimplenciaSimples:

    repositorio_banco = ConsultaExecutor(
        data_inicial=data_inicial,
        data_final=data_final,
        conexao_banco=conexao
    )
    return InadimplenciaSimples(
        data_inicial=data_inicial,
        data_final=data_final,
        repositorio=repositorio_banco
    )


def main():
    # Dados iniciai:
    data_inicial = "2018-01-16"
    data_final = "2027-01-15"
    conexao_banco_resulth = ConexaoBancoDadosResulth()

    # Calcular inadimplência:
    regra_de_negecio = criar_regras_de_negocio(
        data_inicial,
        data_final,
        conexao_banco_resulth
    )
    taxa_inadimplencia = regra_de_negecio.calcular_taxa_inadimplencia()
    return taxa_inadimplencia


if __name__ == "__main__":
    resultado = main()
    print(f"Taxa de Inadimplência: {resultado:2.2f}%")
