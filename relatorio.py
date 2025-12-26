import pandas as pd
import pickle
from pathlib import Path


def extrair_numero(nome_oleo):
    if "GRANEL" in nome_oleo:
        return 1
    numero_int = ""
    for numero in nome_oleo.split()[-1]:
        if numero.isdigit():
            numero_int += numero
    if numero_int == "":
        return None
    elif numero_int == 500:
        return 0.5
    elif numero_int == 200:
        return 0.2
    elif numero_int == 100:
        return 0.1
    return int(numero_int)


def gerar_resultado():
    pd.set_option('display.precision', 0)
    pd.set_option('float_format', '{:.0f}'.format)
    df_oleo = pd.read_csv(CAMINHO_ARQUIVO)
    total = []
    for indice, linha in df_oleo.iterrows():
        try:
            total.append(linha["QUANTIDADE"] * extrair_numero(linha["DESCRICAO"]))
        except TypeError:
            total.append(0)
    df_oleo["TOTAL"] = total
    df_oleo['DATA'] = pd.to_datetime(df_oleo['DATA'])
    resultado = df_oleo.groupby(
        df_oleo['DATA'].dt.year)['TOTAL'].sum().reset_index()
    resultado.columns = ['ano', 'total_vendas_em_litros']
    return resultado


# Caminho para o arquivo do sistema em .csv
CAMINHO_ARQUIVO = Path("oleo.csv")

# Salva variável
resultado = gerar_resultado()
with open('resultado.pkl', 'wb') as f:
    pickle.dump(resultado, f)


if __name__ == "__main__":
    r = gerar_resultado()
    print(r)
