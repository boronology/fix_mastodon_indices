import connection_info
import accounts


def main():
    conn = connection_info.ConnectionInfo(
        "localhost", 5432, "username", "databasename", "password"
    )
    accounts_method = accounts.AccountsMethod(conn)
    accounts_method.execute()


if __name__ == "__main__":
    main()
