from abc import ABC, abstractmethod


class IRepositorioBancoAbstrato(ABC):
    @abstractmethod
    def consultar_dados(self): pass

    @abstractmethod
    def processar_dados(self): pass


class RepositorioQuery(ABC):
    @abstractmethod
    def conultar_saldo_contas_a_receber(self) -> list:
        pass

    @abstractmethod
    def consultar_saldo_clientes_inadimplentes(self) -> list:
        pass
