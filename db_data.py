import sqlite3
from pathlib import Path


def insert_data(dados1=None, dados2=None, dados3=None):
    """
    Função para inserir dados dinamicamente em uma tabela SQLite.

    :param dados: Lista de tuplas contendo os dados a serem inseridos.
    """

    # Caminho
    ROOT_DIR = Path(__file__).parent
    DB_NAME = 'db.areceber.sqlite3'
    DB_FILE = ROOT_DIR / DB_NAME
    TABLE_NAME = 'pmr_giro'
    TABLE_NAME_S = 'saldos'
    TABLE_NAME_INDIMPLENCIA = 'inadimplencia'

    try:
        with sqlite3.connect(DB_FILE) as conexao:
            cursor = conexao.cursor()

            # Cria a tabela se não existir
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} '
                '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'data TEXT, '
                'pmr REAL, '
                'giro REAL, '
                'atraso REAL)'
            )
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {TABLE_NAME_S} '
                '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'DATA TEXT, '
                'REC_VENCIDOS REAL, '
                'REC_NVENCIDOS REAL, '
                'FATURAMENTO REAL)'
            )
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {TABLE_NAME_INDIMPLENCIA} '
                '(id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'DATA TEXT, '
                'SALDO_INADIMPLENTE REAL, '
                'SALDO_A_RECEBER REAL, '
                'TAXA_INADIMPLENCIA REAL)'
            )
            if dados1 is not None:
                # Query SQL fixa com os nomes das colunas
                sql = (
                    f'INSERT INTO {TABLE_NAME} '
                    '(data, pmr, giro, atraso) VALUES (?, ?, ?, ?) '
                )
                # Verifica se é uma lista de tuplas ou uma única tupla
                if isinstance(dados1, list):
                    cursor.executemany(sql, dados1)
                else:
                    cursor.execute(sql, dados1)
                conexao.commit()
            if dados2 is not None:
                # Comandos para inserir dados na tabela Saldos
                sql2 = (
                    f'INSERT INTO {TABLE_NAME_S} '
                    '(DATA, REC_VENCIDOS, REC_NVENCIDOS, FATURAMENTO) VALUES (?, ?, ?, ?) '
                )

                # Verifica se é uma lista de tuplas ou uma única tupla
                if isinstance(dados2, list):
                    cursor.executemany(sql2, dados2)
                else:
                    cursor.execute(sql2, dados2)
                conexao.commit()
            if dados3 is not None:
                # Comandos para inserir dados na tabela Saldos
                sql3_ = (
                    f'INSERT INTO {TABLE_NAME_INDIMPLENCIA} '
                    '(DATA, SALDO_INADIMPLENTE, SALDO_A_RECEBER, TAXA_INADIMPLENCIA) VALUES (?, ?, ?, ?) '
                )

                # Verifica se é uma lista de tuplas ou uma única tupla
                if isinstance(dados3, list):
                    cursor.executemany(sql3_, dados3)
                else:
                    cursor.execute(sql3_, dados3)
                conexao.commit()
    except sqlite3.Error as erro:
        conexao.rollback()
        raise sqlite3.Error(f"Erro ao inserir dados: {erro}") from erro
