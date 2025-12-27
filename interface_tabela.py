from abc import ABC, abstractmethod
import pandas as pd


class IRepositorioTabelaAbstrato(ABC):
    @abstractmethod
    def carregar_estoque(self) -> pd.DataFrame: pass

    @abstractmethod
    def retornar_serie_total_estoque(self) -> pd.Series: pass

    @abstractmethod
    def retornar_dataframe_estoque(self) -> pd.DataFrame: pass
