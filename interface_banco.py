from abc import ABC, abstractmethod


class IRepositorioBancoAbstrato(ABC):
    @abstractmethod
    def consultar_dados(self): pass

    @abstractmethod
    def processar_dados(self): pass
