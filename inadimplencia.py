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
    def __init__(self, repositorio: RepositorioQuery) -> None:
        self._repositorio = repositorio

    def obter_total_valor_clentes_inadimplentes(self) -> float:
        dados = self._repositorio.consultar_saldo_clientes_inadimplentes()
        saldos_clientes_inadimplentes = [float(saldo[2]) for saldo in dados]
        return sum(saldos_clientes_inadimplentes)

    def obter_saldo_contas_a_receber(self) -> float:
        valor = self._repositorio.conultar_saldo_contas_a_receber()
        return float(valor[0][0])

    def calcular_taxa_inadimplencia(self) -> float:
        saldo_inadimplencia = self.obter_total_valor_clentes_inadimplentes()
        saldo_contas_a_receber = self.obter_saldo_contas_a_receber()
        return (saldo_inadimplencia / saldo_contas_a_receber) * 100
