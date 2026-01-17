from db import db_resulth
from datetime import date
from db_data import insert_data
from contas_a_receber import calcular_dados_inadimplencia


def format_numb(number: str):
    return f'{number:_.2f}'.replace('.', ',').replace('_', '.')


def main():
    resulth = db_resulth()

    # Transformação em float para o Total do Faturamento
    faturamento = round(resulth[0], ndigits=2)
    totalvencido = round(resulth[1], ndigits=2)
    totalreceber = round(resulth[2], ndigits=2)

    # Memorial de cálculo
    prazo_medio_recebimento = ((totalvencido + totalreceber) / faturamento) * 365
    giro = 365 / prazo_medio_recebimento
    atraso = totalvencido / (totalvencido + totalreceber)

    # Dados da inadimplência:
    dados_inadimplencia = calcular_dados_inadimplencia()

    indicadores_contas_recerber = (date.today(), prazo_medio_recebimento, giro, atraso)
    valores_contas_receber = (date.today(), totalvencido, totalreceber, faturamento)
    indicadores_inadimplencia = (
        date.today(),
        dados_inadimplencia["saldo_inadimplente"],
        dados_inadimplencia["saldo_contas_a_receber"],
        dados_inadimplencia["taxa_inadimplencia"]
    )
    insert_data(
        dados1=indicadores_contas_recerber,
        dados2=valores_contas_receber,
        dados3=indicadores_inadimplencia
    )


if __name__ == "__main__":
    main()
