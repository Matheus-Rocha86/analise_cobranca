import sqlite3
from pathlib import Path


class ConexaoRepositorioReceberSqlite3:
    def __init__(self) -> None:
        ROOT_DIR = Path(__file__).parent
        DB_NAME = 'db.areceber.sqlite3'
        self.DB_FILE = ROOT_DIR / DB_NAME

    def conectar_sqlite(self):
        return sqlite3.connect(self.DB_FILE)
