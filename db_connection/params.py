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

    # Load environment variables from .env
    BASE_DIR = Path("/home/bb-8/etl-workshop-3")
    load_dotenv(BASE_DIR / ".env")


    # Logs
    log_name = BASE_DIR / "log" / "dump.log"

    # Control flags
    force_execution = True

    # Database credentials from .env
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")