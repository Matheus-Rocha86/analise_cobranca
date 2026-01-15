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


def calcular_dados_inadimplencia():
    # Dados iniciai:
    data_inicial = "2000-12-31"
    data_final = "2050-12-31"
    conexao_banco_resulth = ConexaoBancoDadosResulth()

    # Calcular inadimplência:
    regra_de_negecio = criar_regras_de_negocio(
        data_inicial,
        data_final,
        conexao_banco_resulth
    )
    resultado = {
        "saldo_inadimplente": regra_de_negecio.obter_total_valor_clentes_inadimplentes(),
        "saldo_contas_a_receber": regra_de_negecio.obter_saldo_contas_a_receber(),
        "taxa_inadimplencia": regra_de_negecio.calcular_taxa_inadimplencia()
    }
    return resultado


if __name__ == "__main__":
    resultado = calcular_dados_inadimplencia()
    print(resultado["saldo_inadimplente"])
    print(resultado["saldo_contas_a_receber"])
    print(resultado["taxa_inadimplencia"])
