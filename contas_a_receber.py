from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from repositorio_de_banco import ConsultaExecutor
from inadimplencia import InadimplenciaSimples


def criar_regras_de_negocio(
        repositorio_banco: ConsultaExecutor) -> InadimplenciaSimples:
    return InadimplenciaSimples(repositorio=repositorio_banco)


def calcular_dados_inadimplencia(regra_de_negecio: InadimplenciaSimples):
    return {
        "saldo_inadimplente": int(regra_de_negecio.obter_total_valor_clentes_inadimplentes()),
        "saldo_contas_a_receber": int(regra_de_negecio.obter_saldo_contas_a_receber()),
        "taxa_inadimplencia": round(regra_de_negecio.calcular_taxa_inadimplencia(), 2)
    }


if __name__ == "__main__":
    repositorio_banco = ConsultaExecutor(
        data_inicial="2000-12-31",
        data_final="2100-12-31",
        conexao_banco=ConexaoBancoDadosResulth()
    )
    regras_de_negocio = criar_regras_de_negocio(repositorio_banco)
    resultados = calcular_dados_inadimplencia(regras_de_negocio)
    for k, v in resultados.items():
        print(f"{k}: {v}")
