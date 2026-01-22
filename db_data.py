import sqlite3
from conexao_repositorio_receber_sqlite import ConexaoRepositorioReceberSqlite3


class RepositorioReceberSqlite3:
    def __init__(self, conexao_sqlite3: ConexaoRepositorioReceberSqlite3) -> None:
        self._conexao_sqlite3 = conexao_sqlite3

    def _executar_sql(self, slq: str, parametros: list):
        try:
            with self._conexao_sqlite3.conectar_sqlite() as conn:
                cursor = conn.cursor()
                cursor.executemany(slq, parametros)
                conn.commit()
        except sqlite3.Error as erro:
            conn.rollback()
            raise sqlite3.Error(f"Erro ao inserir dados: {erro}") from erro

    def salvar_pmr_giro(self, dados_pmr: list):
        """Salva dados na tabela pmr_giro"""
        sql = (
            'INSERT INTO pmr_giro '
            '(data, pmr, giro, atraso) VALUES (?, ?, ?, ?) '
        )
        self._executar_sql(sql, dados_pmr)

    def salvar_saldos(self, dados_saldos: list):
        """Salva dados na tabela saldos"""
        sql = (
            'INSERT INTO saldos '
            '(DATA, REC_VENCIDOS, REC_NVENCIDOS, FATURAMENTO) VALUES (?, ?, ?, ?) '
        )
        self._executar_sql(sql, dados_saldos)

    def salvar_inadimplencia(self, dados_inadimplencia: list):
        """Salva dados na tabela inadimplencia"""
        sql = (
            'INSERT INTO inadimplencia '
            '(DATA, SALDO_INADIMPLENTE, SALDO_A_RECEBER, TAXA_INADIMPLENCIA) VALUES (?, ?, ?, ?) '
        )
        self._executar_sql(sql, dados_inadimplencia)
