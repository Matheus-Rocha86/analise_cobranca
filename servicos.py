import pandas as pd


df1 = pd.read_csv('servicosdados.csv')
#print(f"média_janeiro : {round(df1["TOTAL_VALOR"][0] / df1["TOTAL_SERVICOS"][0], 2)}")

# Gleidson - análises
os_gleidson = pd.read_csv('tabela_os_gleidson.csv')
print(os_gleidson.describe())
print(f"número que mais se repete\n{os_gleidson['QT_SERVICO_FINALIZADO'].mode()[0]}")