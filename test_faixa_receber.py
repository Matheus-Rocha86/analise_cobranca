import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from repositorio_de_banco import FaixaReceber, ConsultaExecutor
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth


class TestFaixaReceber:
    def test_inicializacao_com_valores(self):
        faixa = FaixaReceber(faixa_1_a_30_dias=True)
        assert faixa._selecionar_faixa_dias() == "faixa_1_a_30_dias"

    def test_inicializacao_com_valores_todos_false(self):
        faixa = FaixaReceber()
        assert faixa._selecionar_faixa_dias() == 0


class TestConsultaExcutor:
    def test_consultar_clientes_faixa_atraso_dias(self):
        repositorio = ConsultaExecutor(
            data_inicial="2000-12-31",
            data_final="2100-12-31",
            conexao_banco=ConexaoBancoDadosResulth()
        )
        faixa = FaixaReceber(faixa_31_a_60_dias=True)
        query = faixa.acessar_faixa_clientes_em_atraso()
        resultado = repositorio.consultar_clientes_faixa_atraso_dias(query)

        assert type(resultado) is list
