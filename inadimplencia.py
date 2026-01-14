from abc import ABC, abstractmethod
from conexao_banco_dados_resulth import Conexao
from interface_banco import IRepositorioBancoAbstrato


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
                 conexao_banco_resulth: Conexao,
                 repositorio: IRepositorioBancoAbstrato) -> None:
        self.conexao_banco_resulth = conexao_banco_resulth
        self.repositorio = repositorio

    def obter_total_valor_clentes_inadimplentes(self) -> float:
        return self.total_inadimplencia

    def obter_saldo_contas_a_receber(self) -> float:
        return self.saldo_contas_receber

    def calcular_taxa_inadimplencia(self) -> float:
        if self.saldo_contas_receber == 0:
            return 0.0
        return (self.total_inadimplencia / self.saldo_contas_receber) * 100
