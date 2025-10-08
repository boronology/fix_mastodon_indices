class ConnectionInfo:
    def __init__(
        self, host: str, port: int, username: str, database: str, password: str
    ) -> None:
        self.__host = host
        self.__port = port
        self.__username = username
        self.__database = database
        self.__password = password

    @property
    def connection_str(self) -> str:
        return f"host={self.__host} port={self.__port} user={self.__username} dbname={self.__database} password={self.__password}"
