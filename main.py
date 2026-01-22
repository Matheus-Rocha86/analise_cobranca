from db import db_resulth
from datetime import date
from db_data import RepositorioReceberSqlite3
from contas_a_receber import criar_regras_de_negocio, calcular_dados_inadimplencia
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from conexao_repositorio_receber_sqlite import ConexaoRepositorioReceberSqlite3
from repositorio_de_banco import ConsultaExecutor


def obter_saldos_receber():
    data_hoje = date.today().strftime("%Y-%m-%d")
    resulth = db_resulth()

    faturamento = float(round(resulth[0], ndigits=2))
    totalvencido = float(round(resulth[1], ndigits=2))
    totalreceber = float(round(resulth[2], ndigits=2))

    return data_hoje, faturamento, totalvencido, totalreceber


def calcular_indicadores_receber(saldos):
    # Memorial de cálculo
    dias_ano = 365

    prazo_medio_recebimento = ((saldos[2] + saldos[3]) / saldos[1]) * dias_ano
    giro = dias_ano / prazo_medio_recebimento
    atraso = saldos[2] / (saldos[2] + saldos[3])

    return prazo_medio_recebimento, giro, atraso


def calcular_indicadores_inadimplencia(repositorio_resulth: ConsultaExecutor):
    regras_de_negocio = criar_regras_de_negocio(repositorio_resulth)

    indicadores_inadimplencia = calcular_dados_inadimplencia(regras_de_negocio)

    saldo_inadimplente = int(indicadores_inadimplencia["saldo_inadimplente"])
    saldo_contas_a_receber = int(indicadores_inadimplencia["saldo_contas_a_receber"])
    taxa_inadimplencia = float(indicadores_inadimplencia["taxa_inadimplencia"])

    return saldo_inadimplente, saldo_contas_a_receber, taxa_inadimplencia


def salvar_dados_receber_sqlite3(resultados: dict, repositorio: RepositorioReceberSqlite3):
    repositorio.salvar_pmr_giro(resultados["indicadores_pmr"])
    repositorio.salvar_saldos(resultados["valores_contas_receber"])
    repositorio.salvar_inadimplencia(resultados["indicadores_inadimplencia"])
    print("*** Dados salvos com sucesso! ***")


def agrupar_resultados(saldos_principais_receber, indicares_receber, indicadores_inadimplencia):
    data_hoje, faturamento, totalvencido, totalreceber = saldos_principais_receber
    prazo_medio_recebimento, giro, atraso = indicares_receber
    saldo_inadimplente, saldo_contas_a_receber, taxa_inadimplencia = indicadores_inadimplencia

    return {
        "indicadores_pmr": [(data_hoje, prazo_medio_recebimento, giro, atraso)],
        "valores_contas_receber": [(data_hoje, totalvencido, totalreceber, faturamento)],
        "indicadores_inadimplencia": [(data_hoje, saldo_inadimplente, saldo_contas_a_receber, taxa_inadimplencia)]
    }


def main():
    data_inicial = "2000-12-31"
    data_final = "2100-12-31"

    conexao_banco_resulth = ConexaoBancoDadosResulth()
    conexao_banco_receber = ConexaoRepositorioReceberSqlite3()

    repositorio_resulth = ConsultaExecutor(
        data_inicial=data_inicial,
        data_final=data_final,
        conexao_banco=conexao_banco_resulth
    )
    repositorio_receber = RepositorioReceberSqlite3(
        conexao_sqlite3=conexao_banco_receber
    )

    saldos_principais_receber = obter_saldos_receber()

    indicares_receber = calcular_indicadores_receber(saldos_principais_receber)

    indicadores_inadimplencia = calcular_indicadores_inadimplencia(repositorio_resulth)

    resultados = agrupar_resultados(
        saldos_principais_receber,
        indicares_receber,
        indicadores_inadimplencia
    )

    salvar_dados_receber_sqlite3(resultados, repositorio_receber)


if __name__ == "__main__":
    resultado = main()
    print("\n")
