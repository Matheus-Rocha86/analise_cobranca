import json
import os
from datetime import datetime
import locale


def to_save_json(categories, data):
    arquivo_json = 'dados_mensais.json'
    if os.path.exists(arquivo_json):
        with open(arquivo_json, "r", encoding="utf-8") as file:
            dados_existentes = json.load(file)

    dados_novos = {
        f'{categories[0]}': data[0],
        f'{categories[1]}': data[1],
        f'{categories[2]}': data[2],
        f'{categories[3]}': data[3],
        f'{categories[4]}': data[4]
    }
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
    mes_atual = datetime.now().strftime("%B")

    dados_existentes[mes_atual] = dados_novos
    with open('dados_mensais.json', 'w') as file:
        json.dump(
            dados_existentes,
            file,
            indent=4,
            ensure_ascii=False
        )
