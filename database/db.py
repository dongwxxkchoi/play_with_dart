import sqlite3
import asyncio

class DB:
    def __init__(self, db_name: str):
        self.__db_name = db_name
        self.__connection = None
        self.__cursor = None

    def open_connection(self) -> None:
        try:
            self.__connection = sqlite3.connect(self.__db_name)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            print(e)
            print("failed to open the connection")

    def commit_connection(self) -> None:
        self.__connection.commit()

    def close_connection(self) -> None:
        self.__connection.close()

    def run_query(self, query: str):
        self.__cursor.execute(query)

    def run_query_with_params(self, query: str, params: tuple):
        self.__cursor.execute(query, params)

    def run_fetch(self, query: str):
        res = self.__cursor.execute(query)
        fetched_data = res.fetchall()

        return fetched_data

    def run_fetch_with_params(self, query: str, params: tuple):
        res = self.__cursor.execute(query, params)
        fetched_data = res.fetchall()

        return fetched_data
