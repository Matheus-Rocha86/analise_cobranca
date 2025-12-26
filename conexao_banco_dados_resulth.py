import fdb


class ConexaoBancoDadosResulth:
    """Classe especializada para operações no banco Resulth."""
    def __init__(self) -> None:
        self._connection_params = {
            'host': 'resulthserv',
            'database': 'c:\\ResWinCS\\Banco\\RESULTH.FB',
            'user': 'sysdba',
            'password': 'masterkey'
        }

    def get_connection(self):
        """Retorna uma conexão com o banco de dados."""
        return fdb.connect(**self._connection_params)
