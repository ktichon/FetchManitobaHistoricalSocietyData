"""
This module is used to reuse code on the database module
"""

import sqlite3
import logging

class DBCM:
    """Context Manger to use database"""

    logger = logging.getLogger("main." + __name__)

    def __init__(self, database_name):
        """Initializes the database and cursor"""
        try:
            self.database_name = database_name
        except Exception as error:
            self.logger.error("DBCM/init: %s", error)
            self.database_name = None
        self.connect = None
        self.cursor = None

    def __enter__(self):
        """Returns the cursor"""
        try:
            self.connect = sqlite3.connect(self.database_name)
            self.cursor = self.connect.cursor()
        except Exception as error:
            self.logger.error("DBCM/enter: %s", error)
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_trace):
        """Closes the database"""
        try:
            self.connect.commit()
        except Exception as error:
            self.logger.error("DBCM/exit: %s", error)
        finally:
            self.cursor.close()
            self.connect.close()
