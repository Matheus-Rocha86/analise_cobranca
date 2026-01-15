from abc import ABC, abstractmethod
from interface_banco import RepositorioQuery


class Inadimplencia(ABC):
    @abstractmethod
    def obter_total_valor_clentes_inadimplentes(self) -> float:
        """Retorna o valor total em atraso."""
        pass

    @abstractmethod
    def calcular_taxa_inadimplencia(self) -> float:
        """Retorna a taxa de inadimplência."""
        pass


class InadimplenciaSimples(Inadimplencia):
    def __init__(self,
                 data_inicial: str,
                 data_final: str,
                 repositorio: RepositorioQuery) -> None:
        self.data_inicial = data_inicial
        self.data_final = data_final
        self._repositorio = repositorio

    def obter_total_valor_clentes_inadimplentes(self) -> float:
        valor = self._repositorio.consultar_saldo_clientes_inadimplentes()
        return float(valor[0][0])

    def obter_saldo_contas_a_receber(self) -> float:
        valor = self._repositorio.conultar_saldo_contas_a_receber()
        return float(valor[0][0])

    def calcular_taxa_inadimplencia(self) -> float:
        saldo_inadimplencia = self.obter_total_valor_clentes_inadimplentes()
        saldo_contas_a_receber = self.obter_saldo_contas_a_receber()
        return (saldo_inadimplencia / saldo_contas_a_receber) * 100
