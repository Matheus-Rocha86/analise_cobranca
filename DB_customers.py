from resuthquery import ResulthDatabase
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter
import locale
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')


# Configuração do banco
resulth_db = ResulthDatabase()

# Obtém resumo financeiro
faturamento, vencidos, a_vencer = resulth_db.get_financial_summary()

#print(f"Faturamento: R$ {faturamento:,.2f}")
#print(f"Vencidos: R$ {vencidos:,.2f}")
#print(f"A vencer: R$ {a_vencer:,.2f}")

# Criar o DataFrame no Pandas
# Formatação numérica de duas casas decimais
pd.options.display.float_format = '{:.2f}'.format
df = pd.DataFrame(
    resulth_db.get_overdue_balances(),
    columns=['COD', 'NOME', 'DATA', 'VLRDOCTO', 'SALDO', 'DIFF_DATA']
)

# Realiza o produto da coluna SALDO com a coluna DIFF_DATA
prod_saldo_diffdata = []

for linha in df.itertuples():
    prod_saldo_diffdata.append(linha.SALDO * linha.DIFF_DATA)

df['PRODUTO'] = prod_saldo_diffdata

# Realiza o agrupamento dos clientes
df_gruouped = df.groupby(['COD', 'NOME']).agg({
    'SALDO': 'sum',
    'PRODUTO': 'sum'
}).reset_index()

# Cálculo do PMR geral
pmr_global = ((vencidos + a_vencer) / faturamento) * 365

# Cálculo do PMR por cliente
pmr_customers = []

for row in df_gruouped.itertuples():
    pmr_customers.append(row.PRODUTO / row.SALDO)

# Visualização dos dados
df_gruouped['PMR'] = pmr_customers

df_finally = df_gruouped[['COD', 'NOME', 'SALDO', 'PMR']]

gp = df_finally.loc[df_finally['PMR'] > pmr_global].reset_index()

# Imprimir
formatted_saldo = gp['SALDO'].apply(lambda x: locale.format_string('%10.2f', x, grouping=True))

print(gp[['COD', 'NOME']].assign(SALDO=formatted_saldo).sort_values(by='NOME', ascending=True).to_string(index=False))

# Visualização em gráficos
# Quantidade de contas vencidas
# <30 dias
print(df_finally['PMR'].value_counts(bins=5))

print(df_finally['PMR'].describe())

print(df_finally['SALDO'].describe())

# Plots
# Faixas de valores
track_1 = df_finally.loc[(df_finally['PMR'] <= 30)].reset_index()  # Até 30 dias

track_2 = df_finally.loc[(df_finally['PMR'] > 30) & (df_finally['PMR'] <= 60)].reset_index()  # De 30 até 60 dias

track_3 = df_finally.loc[(df_finally['PMR'] > 60) & (df_finally['PMR'] <= 90)].reset_index()  # De 60 até 90 dias

track_4 = df_finally.loc[(df_finally['PMR'] > 90) & (df_finally['PMR'] <= 120)].reset_index(drop=True)  # De 90 até 120 dias

track_5 = df_finally.loc[(df_finally['PMR'] > 120) & (df_finally['PMR'] <= 180)].reset_index()  # De 120 até 180 dias

# Visualizar clientes em atraso em uma faixa específica
atrasos = track_5['SALDO'].apply(lambda x: locale.format_string('%10.2f', x, grouping=True))

print(track_5[['COD', 'NOME']].assign(SALDO=atrasos).sort_values(by='NOME', ascending=True).to_string(index=False))

# Totais das faixas
tot_track_1 = track_1['SALDO'].sum()

tot_track_2 = round(track_2['SALDO'].sum(), ndigits=2)

tot_track_3 = track_3['SALDO'].sum()

tot_track_4 = track_4['SALDO'].sum()

tot_track_5 = track_5['SALDO'].sum()

categories = ['<30 dias', 'De 30 a 60 dias', 'De 60 a 90 dias', 'De 90 a 120 dias', 'De 120 dias>']

data = [tot_track_1, tot_track_2, tot_track_3, tot_track_4, tot_track_5]


def retornar_valores_em_atraso(dias_de_atraso: int) -> float:
    """Retorna o valor total em atraso conforme a faixa de dias informada."""
    match dias_de_atraso:
        case 30:
            return tot_track_1
        case 60:
            return tot_track_2
        case 90:
            return tot_track_3
        case 120:
            return tot_track_4
        case 180:
            return tot_track_5
        case _:
            return 0.0


# Gráfico de barras
def adicionar_rotulos(bar_plot, data):
    for i, valor in enumerate(data):
        plt.text(i, valor, f'{round(valor, ndigits=2):_}'.replace('.', ',').replace('_', '.'), ha='center', va='bottom')


barr = plt.bar(categories, data, color='skyblue')

adicionar_rotulos(plt.bar(categories, data), data)

plt.title('Total em atraso por tempo em dias')

plt.ylabel('Total em atraso (R$)')

# Gráfico de pizza
#myexplode = [0.025, 0, 0, 0, 0]
#plt.pie(data, autopct='%1.1f%%', startangle=90, explode=myexplode)
#plt.legend(categories, title="Atrasos:", bbox_to_anchor=(1.2, 0.5), loc='center right')
#plt.tight_layout()
#plt.title('Clientes em atraso', loc='center', fontdict={'size': 24}, y=0.95)
#plt.show()
# Histograma
#def formato_milhar(x, pos):
#    return '{:,.0f}'.format(x).replace(',', '.')
#plt.title('Distribuição da quantidade de clientes pelo saldo em atraso', fontsize=20)
#plt.xlabel('Valor em R$', fontsize=15)
#plt.ylabel('Frequência Absoluta', fontsize=15)
#plt.tick_params(labelsize=12)
#plt.hist(df_gruouped['SALDO'], 30, rwidth=0.9, color='red', alpha=0.7, density=True)
#plt.gca().xaxis.set_major_formatter(FuncFormatter(formato_milhar))
#plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.show()
