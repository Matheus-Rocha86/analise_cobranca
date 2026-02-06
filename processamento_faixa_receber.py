import pandas as pd
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from repositorio_de_banco import FaixaReceber, ConsultaExecutor
from inadimplencia import InadimplenciaSimples
from pprint import pprint


class ProcessamentoFaixaReceber:
    def __init__(self,
                 faixa_lista: FaixaReceber | None = None,
                 inadimplencia: InadimplenciaSimples | None = None,
                 faixas_listas: list[FaixaReceber] | None = None,
                 repositorio: ConsultaExecutor | None = None) -> None:
        self.faixa = faixa_lista
        self.inadimplencia = inadimplencia
        self.faixas_listas = faixas_listas
        self.repositorio = repositorio

    def obter_saldo_total_contas_a_receber(self) -> float:
        if self.inadimplencia is None:
            raise ValueError("InadimplenciaSimples não fornecido.")
        return self.inadimplencia.obter_saldo_contas_a_receber()

    def calcular_total_faixa(self) -> float:
        total = []
        for valor in self.faixa:
            total.append(valor[2])
        return sum(total)

    def _acessar_cod_nomes_clientes(self) -> list:
        cod = [item[0] for item in self.faixa]
        nome = [item[1] for item in self.faixa]
        dados = []
        dados.append(cod)
        dados.append(nome)
        return dados

    def transformar_em_dataframe(self) -> pd.DataFrame:
        clientes = self._acessar_cod_nomes_clientes()
        return pd.DataFrame({"COD": clientes[0], "NOME": clientes[1]})

    def mostrar_totais_a_receber_debitos_em_atraso(self) -> dict:
        resumo = {
            "saldo_total_contas_a_receber": self.obter_saldo_total_contas_a_receber(),
            "saldo_total_debitos_em_atraso": self.calcular_total_faixa()
        }
        return resumo

    def _calcular_total_por_faixa(self, faixa: FaixaReceber) -> float:
        """Método privado que calcula o total para uma faixa específica"""
        query = faixa.acessar_faixa_clientes_em_atraso()
        resultado = self.repositorio.consultar_clientes_faixa_atraso_dias(query)
        return sum([item[2] for item in resultado])

    def obter_valor_total_clientes_em_atraso(self) -> float:
        total = 0.0
        for faixa in self.faixas_listas:
            total += self._calcular_total_por_faixa(faixa)
        return total

    def _obter_total_clientes_em_atraso_em_cada_faixa(self) -> dict:
        totais = {}
        for faixa in self.faixas_listas:
            totais[faixa] = self._calcular_total_por_faixa(faixa)
        return totais

    def mostrar_resumo_totais_faixas(self) -> dict:
        resultado_transformado = {}
        for faixa_obj, valor in self._obter_total_clientes_em_atraso_em_cada_faixa().items():
            chave = faixa_obj._selecionar_faixa_dias()
            resultado_transformado[chave] = valor
        return resultado_transformado


repositorio = ConsultaExecutor(
    data_inicial="2000-12-31",
    data_final="2100-12-31",
    conexao_banco=ConexaoBancoDadosResulth()
)
faixas = [
    FaixaReceber(faixa_1_a_30_dias=True),
    FaixaReceber(faixa_31_a_60_dias=True),
    FaixaReceber(faixa_61_a_90_dias=True),
    FaixaReceber(faixa_91_a_120_dias=True),
    FaixaReceber(faixa_120_acima_dias=True)
]
processamento = ProcessamentoFaixaReceber(
    faixas_listas=faixas,
    repositorio=repositorio
)

totais_faixas_resumo = processamento.mostrar_resumo_totais_faixas()

pprint(totais_faixas_resumo)

# Dataframe de clientes em atraso
print()
print("Faixas disponíveis:")
for i, faixa in enumerate(faixas):
    print(f"faixas[{i}]: {faixa._selecionar_faixa_dias()}")
print()

try:
    digito = int(input('>> Digite o número da faixa de atraso (0 a 4): '))
    if digito < 0 or digito > 4:
        raise ValueError("Número fora do intervalo permitido.")
    elif not isinstance(digito, int):
        raise ValueError("Entrada inválida. Digite um número inteiro.")
    else:
        query = faixas[digito].acessar_faixa_clientes_em_atraso()
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
