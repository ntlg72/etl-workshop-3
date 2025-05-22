from sqlalchemy import create_engine
import logging


logger = logging.getLogger('client.py')


from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

class DatabaseClient:
    """
    A database client using SQLAlchemy for PostgreSQL connections.
    """

    def __init__(self, params):
        """
        Initialize the database connection.
        """
        try:
            # Create the SQLAlchemy engine
            self.engine = create_engine(
                f'postgresql+psycopg2://{params.user}:{params.password}@{params.host}:{params.port}/{params.database}'
            )
            # Test the connection
            self.conn = self.engine.connect()
            logging.info('Successfully connected to the database.')
        except SQLAlchemyError as e:
            logger.error('Failed to connect to the database.')
            logger.error(f'Error details: {e}')

    def close(self):
        """
        Close the database connection.
        """
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                logging.info('Connection to database closed successfully.')
            else:
                logging.warning('No active connection to close.')
        except SQLAlchemyError as e:
            logger.error('Failed to close the database connection.')
            logger.error(f'Error details: {e}')