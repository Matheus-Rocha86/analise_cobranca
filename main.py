from db import db_resulth
from format_num import format_numb
from datetime import date
from db_data import insert_data

resulth = db_resulth()

# Transformação em float para o Total do Faturamento
faturamento = round(resulth[0], ndigits=2)
totalvencido = round(resulth[1], ndigits=2)
totalreceber = round(resulth[2], ndigits=2)

# Memorial de cálculo
prazo_medio_recebimento = ((totalvencido + totalreceber) / faturamento) * 365
giro = 365 / prazo_medio_recebimento
atraso = totalvencido / (totalvencido + totalreceber)

#print(f'Faturamento anual = {format_numb(faturamento)}')
#print(f'Total a receber vencidas = {format_numb(totalvencido)}')
#print(f'Total a receber não vencidas = {format_numb(totalreceber)}')
#print('PMR =', round(prazo_medio_recebimento, ndigits=2))
#print('Giro =', round(giro, ndigits=None))
#print(f'Atraso = {(round(atraso, ndigits=2)) * 100} %')

dados1 = (date.today(), prazo_medio_recebimento, giro, atraso)
dados2 = (date.today(), totalvencido, totalreceber, faturamento)
insert_data(dados1, dados2)
