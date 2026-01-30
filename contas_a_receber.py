from repositorio_de_banco import ConsultaExecutor
from inadimplencia import InadimplenciaSimples


def criar_regras_de_negocio(
        repositorio_banco: ConsultaExecutor) -> InadimplenciaSimples:
    return InadimplenciaSimples(repositorio=repositorio_banco)


def calcular_dados_inadimplencia(regra_de_negecio: InadimplenciaSimples):
    return {
        "saldo_inadimplente": regra_de_negecio.obter_total_valor_clentes_inadimplentes(),
        "saldo_contas_a_receber": regra_de_negecio.obter_saldo_contas_a_receber(),
        "taxa_inadimplencia": round(regra_de_negecio.calcular_taxa_inadimplencia(), 2)
    }
