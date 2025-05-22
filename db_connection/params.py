from dotenv import load_dotenv
import os
from pathlib import Path

class Params:
    """
    Parameters class.

    This class centralizes all configurable parameters in the codebase.

    To use a parameter in other parts of your code, instantiate this class:
    `params = Params()`

    Then pass the `params` object into functions and access parameters as attributes.

    For example, to use the `url` parameter in a function:

    ```
    def func(params):
        url = params.url
    ```
    """

    def __init__(self):
        # Ruta base
        BASE_DIR = Path("/home/ntlg2/etl-workshop-3")

        # Carga el .env desde ruta absoluta
        load_dotenv(BASE_DIR / ".env")

        # Logs
        self.log_name = BASE_DIR / "log" / "dump.log"

        # Control flags
        self.force_execution = True

        # Variables de entorno
        self.user = os.getenv("DB_USERNAME")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.database = os.getenv("DB_DATABASE")

        port = os.getenv("DB_PORT")
        if port is None:
            self.port = 5434
        else:
            self.port = int(port)
