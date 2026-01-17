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
    data_final = "2100-12-31"
    conexao_banco_resulth = ConexaoBancoDadosResulth()

    # Calcular inadimplência:
    regra_de_negecio = criar_regras_de_negocio(
        data_inicial,
        data_final,
        conexao_banco_resulth
    )

    return {
        "saldo_inadimplente": int(regra_de_negecio.obter_total_valor_clentes_inadimplentes()),
        "saldo_contas_a_receber": int(regra_de_negecio.obter_saldo_contas_a_receber()),
        "taxa_inadimplencia": round(regra_de_negecio.calcular_taxa_inadimplencia(), 2)
    }


if __name__ == "__main__":
    resultados = calcular_dados_inadimplencia()
    for k, v in resultados.items():
        print(f"{k}: {v}")
