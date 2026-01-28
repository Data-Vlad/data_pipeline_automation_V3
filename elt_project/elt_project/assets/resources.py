# elt_project/assets/resources.py
import os
from dagster import ConfigurableResource
from sqlalchemy import create_engine

class SQLServerResource(ConfigurableResource):
    """
    A Dagster resource for connecting to a SQL Server database.

    This resource uses environment variables and Windows Credential Manager to
    securely construct a SQLAlchemy engine. It supports both standard and
    ODBC-based connection strings.

    Attributes:
        driver (str): The ODBC driver name (e.g., 'ODBC Driver 17 for SQL Server').
        server (str): The hostname or IP address of the SQL Server instance.
        database (str): The name of the database to connect to.
        username (str, optional): The database username.
        password (str, optional): The database password.
        credential_target (str, optional): The target name in Windows Credential Manager.
        trust_server_certificate (str): Whether to trust the server's certificate ('yes' or 'no').
    """
    driver: str
    server: str
    database: str
    username: str | None = None
    password: str | None = None
    credential_target: str | None = None
    trust_server_certificate: str = "no"

    def get_engine(self):
        """
        Constructs and returns a SQLAlchemy engine.

        It prioritizes using username/password if they are directly provided.
        Otherwise, it constructs a connection string suitable for integrated
        security or other authentication methods.
        """
        if self.username and self.password:
            # Use username/password authentication
            connection_string = (
                f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}"
                f"&TrustServerCertificate={self.trust_server_certificate}"
            )
        else:
            # Use integrated security / trusted connection
            connection_string = (
                f"mssql+pyodbc://@{self.server}/{self.database}?driver={self.driver.replace(' ', '+')}"
                f"&trusted_connection=yes&TrustServerCertificate={self.trust_server_certificate}"
            )
        return create_engine(connection_string)