
from conexao_banco_dados_resulth import ConexaoBancoDadosResulth
from repositorio_de_banco import FaturamentoReceber


def criar_regras_de_negocio(
    data_inicial: str,
    data_final: str
) -> FaturamentoReceber:
    conexao_banco = ConexaoBancoDadosResulth()
    return FaturamentoReceber(
        data_inicial=data_inicial,
        data_final=data_final,
        conexao=conexao_banco,
        saldo_total=True
    )


def main():
    # Configurações iniciais
    data_inicial = '2025-01-01'
    data_final = '2026-01-14'
    # Instancia o objeto de regras de negócio
    regras_de_negocio = criar_regras_de_negocio(
        data_inicial=data_inicial,
        data_final=data_final
    )
    # Executar cálculos
    saldo_total_vencido = regras_de_negocio.processar_dados_total_vendido_clientes()
    return {
        "saldo_total_vencido": saldo_total_vencido
    }


if __name__ == "__main__":
    resultados = main()
    print(resultados)
