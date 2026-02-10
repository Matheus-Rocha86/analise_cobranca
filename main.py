import locale
from db import db_resulth
from datetime import date
from db_data import RepositorioReceberSqlite3
from contas_a_receber import criar_regras_de_negocio, calcular_dados_inadimplencia
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from conexao_repositorio_receber_sqlite import ConexaoRepositorioReceberSqlite3
from repositorio_de_banco import ConsultaExecutor, FaixaReceber
from processamento_faixa_receber import ProcessamentoFaixaReceber
from inadimplencia import InadimplenciaSimples
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')


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


def agrupar_resultados(
        saldos_principais_receber,
        indicares_receber,
        indicadores_inadimplencia
):
    data_hoje, faturamento, totalvencido, totalreceber = saldos_principais_receber
    prazo_medio_recebimento, giro, atraso = indicares_receber
    saldo_inadimplente, saldo_contas_a_receber, taxa_inadimplencia = indicadores_inadimplencia

    return {
        "indicadores_pmr": [(data_hoje, prazo_medio_recebimento, giro, atraso)],
        "valores_contas_receber": [(data_hoje, totalvencido, totalreceber, faturamento)],
        "indicadores_inadimplencia": [(data_hoje, saldo_inadimplente, saldo_contas_a_receber, taxa_inadimplencia)]
    }


def processar_faixa_receber(
        faixas_receber: FaixaReceber,
        processamento: ProcessamentoFaixaReceber,
        repositorio: ConsultaExecutor
):
    totais_faixas_resumo = processamento.mostrar_resumo_totais_faixas()

    print("FAIXAS DE ATRASO - RESUMO")
    print(f"De 1 a 30 dias: {formatar_numeros_em_milhar(totais_faixas_resumo['faixa_1_a_30_dias'])}")
    print(f"De 31 a 60 dias: {formatar_numeros_em_milhar(totais_faixas_resumo['faixa_31_a_60_dias'])}")
    print(f"De 61 a 90 dias: {formatar_numeros_em_milhar(totais_faixas_resumo['faixa_61_a_90_dias'])}")
    print(f"De 91 a 120 dias: {formatar_numeros_em_milhar(totais_faixas_resumo['faixa_91_a_120_dias'])}")
    print(f"Acima de 120 dias: {formatar_numeros_em_milhar(totais_faixas_resumo['faixa_120_acima_dias'])}")
    print(f"Total do débito perdido: {formatar_numeros_em_milhar(totais_faixas_resumo['total_debito_perdido'])}")
    print("=========================================")

    # Dataframe de clientes em atraso
    print()
    print("Faixas disponíveis:")
    for i, faixa in enumerate(faixas_receber):
        print(f"faixas[{i}]: {faixa._selecionar_faixa_dias()}")
    print()

    try:
        digito = int(input('>> Digite o número da faixa de atraso (0 a 5): '))
        if digito < 0 or digito > 5:
            raise ValueError("Número fora do intervalo permitido.")
        elif not isinstance(digito, int):
            raise ValueError("Entrada inválida. Digite um número inteiro.")
        else:
            query = faixas_receber[digito].acessar_faixa_clientes_em_atraso()
            resultado_query = repositorio.consultar_clientes_faixa_atraso_dias(query)
            processamento_faixa = ProcessamentoFaixaReceber(faixa_lista=resultado_query)
            df = processamento_faixa.transformar_em_dataframe()
            print()
            print("DataFrame de clientes por faixa de atraso:")
            print(df)
            print()
    except ValueError as e:
        print(f"Erro: {e}")
        exit()


def obter_saldos_totais(processamento: ProcessamentoFaixaReceber):

    saldo_total = processamento.obter_saldo_total_contas_a_receber()
    saldo_vencido = processamento.obter_valor_total_clientes_em_atraso()
    saldo_nao_vencido = processamento.obter_saldo_nao_vencido()
    print("SALDOS PRINCIPAIS - CONTAS A RECEBER")
    print("=========================================")
    print(f"Saldo vencido: {formatar_numeros_em_milhar(saldo_vencido)}")
    print(f"Saldo não vencido: {formatar_numeros_em_milhar(saldo_nao_vencido)}")
    print(f"Saldo total a receber: {formatar_numeros_em_milhar(saldo_total)}")
    print("=========================================")
    return {
        "saldo_total": saldo_total,
        "saldo_vencido": saldo_vencido,
        "saldo_nao_vencido": saldo_nao_vencido
    }


def formatar_numeros_em_milhar(valor: float) -> str:
    return locale.format_string("%.0f", valor, grouping=True)

# FUNÇÃO MAIN() - PONTO DE ENTRADA DO PROGRAMA

def main():
    # Configurações iniciais e definição de objetos de acesso a dados
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

    faixas_receber = [
        FaixaReceber(faixa_1_a_30_dias=True),
        FaixaReceber(faixa_31_a_60_dias=True),
        FaixaReceber(faixa_61_a_90_dias=True),
        FaixaReceber(faixa_91_a_120_dias=True),
        FaixaReceber(faixa_120_acima_dias=True),
        FaixaReceber(faixa_debito_perdido=True)
    ]

    inadimplencia = InadimplenciaSimples(repositorio_resulth)

    processamento = ProcessamentoFaixaReceber(
        faixas_listas=faixas_receber,
        repositorio=repositorio_resulth,
        inadimplencia=inadimplencia
    )

    # FUNÇÕES PRINCIPAIS

    saldos_principais_receber = obter_saldos_receber()

    indicares_receber = calcular_indicadores_receber(saldos_principais_receber)

    indicadores_inadimplencia = calcular_indicadores_inadimplencia(repositorio_resulth)

    resultados = agrupar_resultados(
        saldos_principais_receber,
        indicares_receber,
        indicadores_inadimplencia
    )

    #salvar_dados_receber_sqlite3(resultados, repositorio_recebe

    obter_saldos_totais(processamento)

    processar_faixa_receber(faixas_receber, processamento, repositorio_resulth)


if __name__ == "__main__":
    resultado = main()
    print("\n")
