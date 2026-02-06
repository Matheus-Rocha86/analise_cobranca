import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from repositorio_de_banco import FaixaReceber, ConsultaExecutor
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from processamento_faixa_receber import ProcessamentoFaixaReceber
from inadimplencia import InadimplenciaSimples


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


class TestProcessamentoFaixaReceber:
    def test_calcular_total_faixa(self):
        repositorio = ConsultaExecutor(
            data_inicial="2000-12-31",
            data_final="2100-12-31",
            conexao_banco=ConexaoBancoDadosResulth()
        )

        faixa = FaixaReceber(faixa_1_a_30_dias=True)

        query = faixa.acessar_faixa_clientes_em_atraso()

        resultado = repositorio.consultar_clientes_faixa_atraso_dias(query)

        processamento = ProcessamentoFaixaReceber(resultado)

        total_faixa = processamento.calcular_total_faixa()

        assert type(total_faixa) is float
        assert total_faixa > 0.0

    def test_acessar_cod_nomes_clientes(self):
        repositorio = ConsultaExecutor(
            data_inicial="2000-12-31",
            data_final="2100-12-31",
            conexao_banco=ConexaoBancoDadosResulth()
        )

        faixa = FaixaReceber(faixa_31_a_60_dias=True)

        query = faixa.acessar_faixa_clientes_em_atraso()

        faixa = repositorio.consultar_clientes_faixa_atraso_dias(query)

        processamento = ProcessamentoFaixaReceber(faixa)

        cod_nomes = processamento._acessar_cod_nomes_clientes()

        assert type(cod_nomes) is list
        assert type(cod_nomes[0]) is list
        assert type(cod_nomes[1]) is list

    def test_transformar_em_dataframe(self):
        repositorio = ConsultaExecutor(
            data_inicial="2000-12-31",
            data_final="2100-12-31",
            conexao_banco=ConexaoBancoDadosResulth()
        )

        faixa = FaixaReceber(faixa_31_a_60_dias=True)

        query = faixa.acessar_faixa_clientes_em_atraso()

        faixa = repositorio.consultar_clientes_faixa_atraso_dias(query)

        processamento = ProcessamentoFaixaReceber(faixa)

        df = processamento.transformar_em_dataframe()

        assert type(df).__name__ == "DataFrame"
        assert "COD" in df.columns
        assert "NOME" in df.columns

    def test_mostrar_resumo_totais_faixas(self):
        repositorio = ConsultaExecutor(
            data_inicial="2000-12-31",
            data_final="2100-12-31",
            conexao_banco=ConexaoBancoDadosResulth()
        )

        faixa = FaixaReceber(faixa_1_a_30_dias=True)

        query = faixa.acessar_faixa_clientes_em_atraso()

        faixa = repositorio.consultar_clientes_faixa_atraso_dias(query)

        inadimplencia = InadimplenciaSimples(repositorio)

        processamento = ProcessamentoFaixaReceber(faixa, inadimplencia)

        resumo = processamento.mostrar_totais_a_receber_debitos_em_atraso()

        assert type(resumo) is dict
        assert resumo["saldo_total_contas_a_receber"] > 0.0
        assert resumo["saldo_total_debitos_em_atraso"] > 0.0

    def test_obter_valor_total_clientes_em_atraso(self):
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

        total_atraso = processamento.obter_valor_total_clientes_em_atraso()

        assert type(total_atraso) is float
        assert total_atraso > 0.0

    def test_obter_total_clientes_em_atraso_em_cada_faixa(self):
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

        totais_faixas = processamento.mostrar_resumo_totais_faixas()

        assert type(totais_faixas) is dict

        for chave, valor in totais_faixas.items():
            assert type(chave) is str
            assert type(valor) is float
            assert valor >= 0.0
