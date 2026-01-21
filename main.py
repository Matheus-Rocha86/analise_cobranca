from db import db_resulth
from datetime import date
from db_data import insert_data
from contas_a_receber import criar_regras_de_negocio, calcular_dados_inadimplencia
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from repositorio_de_banco import ConsultaExecutor


def format_numb(number: str):
    return f'{number:_.2f}'.replace('.', ',').replace('_', '.')


def main():
    data_hoje = date.today()
    resulth = db_resulth()

    # Transformação em float para o Total do Faturamento
    faturamento = round(resulth[0], ndigits=2)
    totalvencido = round(resulth[1], ndigits=2)
    totalreceber = round(resulth[2], ndigits=2)

    # Memorial de cálculo
    prazo_medio_recebimento = ((totalvencido + totalreceber) / faturamento) * 365
    giro = 365 / prazo_medio_recebimento
    atraso = totalvencido / (totalvencido + totalreceber)

    # Calculando o saldo da inadimplência:
    repositorio_banco = ConsultaExecutor(
        data_inicial="2000-12-31",
        data_final="2100-12-31",
        conexao_banco=ConexaoBancoDadosResulth()
    )
    regras_de_negocio = criar_regras_de_negocio(repositorio_banco)
    indicadores_inadimplencia = calcular_dados_inadimplencia(regras_de_negocio)
    saldo_inadimplente = indicadores_inadimplencia["saldo_inadimplente"]
    saldo_contas_a_receber = indicadores_inadimplencia["saldo_contas_a_receber"]
    taxa_inadimplencia = indicadores_inadimplencia["taxa_inadimplencia"]

    indicadores_contas_recerber = (
        data_hoje,
        prazo_medio_recebimento,
        giro,
        atraso
    )
    valores_contas_receber = (
        data_hoje,
        totalvencido,
        totalreceber,
        faturamento
    )
    indicadores_inadimplencia = (
        data_hoje,
        saldo_inadimplente,
        saldo_contas_a_receber,
        taxa_inadimplencia
    )
    insert_data(
        dados1=indicadores_contas_recerber,
        dados2=valores_contas_receber,
        dados3=indicadores_inadimplencia
    )


if __name__ == "__main__":
    main()
