
from datetime import datetime
import logging
import logging.handlers
from dbcm import DBCM
import os
import glob
from os.path import abspath, dirname, join

class DBOperations:
    """Store and retrive site data"""

    logger = logging.getLogger("main." + __name__)
    allTypes = ["Featured Site", "Museum or Archives", "Building", "Monument", "Cemetery", "Location","Other"]

    def __init__(self):
        """Initializes varibles that will be used throught the class"""
        self.database = "historicalSiteData.db"


    def initialize_db(self):
        """Initializes the database"""
        with DBCM(self.database) as cursor:
            try:
                cursor.execute("""create table if not exists manitobaHistoricalSite
                (site_id INTEGER primary key not null,
                name TEXT,
                address TEXT,
                main_type INTEGER,
                latitude REAL not null,
                longitude REAL not null,
                province TEXT,
                municipality TEXT,
                description TEXT,
                site_url TEXT,
                keywords TEXT,
                import_date TEXT

                );""")

                cursor.execute("""create table if not exists sitePhotos
                (photo_id INTEGER primary key autoincrement not null,
                site_id INTEGER not null,
                photo_name TEXT,
                width INTEGER,
                height INTEGER,
                photo_url TEXT,
                info TEXT,
                import_date TEXT
                );""")

                cursor.execute("""create table if not exists siteSource
                (source_id INTEGER primary key autoincrement not null,
                site_id INTEGER not null,
                info TEXT,
                import_date TEXT
                );""")


                #cursor.execute("""create table if not exists siteKeyword
                #(keyword_id INTEGER primary key autoincrement not null,
                #site_id INTEGER not null,
                #keyword TEXT,
                #import_date TEXT
                #);""")

                cursor.execute("""create table if not exists siteWithType
                (
                site_with_type_id INTEGER primary key autoincrement not null,
                site_type_id INTEGER  not null,
                site_id INTEGER not null,
                import_date TEXT
                );""")

                cursor.execute("""create table if not exists siteType
                (site_type_id INTEGER primary key,
                type TEXT,
                import_date TEXT
                );""")
            except Exception as error:
                self.logger.error('DBOperations/initialize_db: %s', error)

    def purge_data(self):
      """Removes all data from the db"""
      with DBCM(self.database) as cursor:
          try:

              cursor.execute("""DELETE FROM manitobaHistoricalSite;""")
              cursor.execute("""DELETE FROM sitePhotos;""")
              cursor.execute("""DELETE FROM siteSource;""")
              cursor.execute("""DELETE FROM siteType;""")
              cursor.execute("""DELETE FROM siteWithType;""")
              #cursor.execute("""DELETE FROM siteKeyword;""")

          except Exception as error:
              self.logger.error('DBOperations/purge_data: %s', error)





    def manitoba_historical_website_save_data(self, historical_sites_list):
        """Saves the data from the Manitoba Historical Society"""
        try:
            #sql = """SELECT TOP 1 site_id FROM historicalSite WHERE streetName = ? AND streetNumber = ?"""

            insert_site_sql =  """INSERT OR IGNORE into manitobaHistoricalSite
            (site_id, name, address, main_type,  latitude, longitude, province, municipality, description, keywords, site_url, import_date)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"""

            insert_photo_sql =  """INSERT OR IGNORE into sitePhotos
            (site_id, photo_name, width, height, photo_url, info, import_date)
            values (?, ?, ?, ?, ?, ?, ?)"""

            insert_source_sql =  """INSERT OR IGNORE into siteSource
            (site_id, info, import_date)
            values (?, ?, ?)"""

            insert_type_sql =  """INSERT OR IGNORE into siteType
            (site_type_id, type, import_date)
            values (?, ?, ?)"""

            insert_site_with_type_sql =  """INSERT OR IGNORE into siteWithType
            (site_type_id, site_id, import_date)
            values (?, ?, ?)"""

            with DBCM(self.database) as cursor:
                print("Insert data from Manitoba Historical Society to database")
                before_insert = cursor.execute("SELECT COUNT() FROM manitobaHistoricalSite").fetchone()[0]

                #Insert the 7 types into the database
                typeID = 1
                for type in self.allTypes:
                    try:
                        cursor.execute(insert_type_sql, (typeID, type, datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                    except Exception as error:
                        self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database/Save Site Types: %s', error)
                    typeID += 1

                for newSite in historical_sites_list:
                    try:
                        cursor.execute(insert_site_sql, ( newSite["site_id"], newSite["site_name"], newSite["address"], newSite["types"][0], newSite["latitude"], newSite["longitude"] , "MB" , newSite["municipality"], newSite["description"], newSite["keywords"], newSite["url"] , datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                        cursor.executemany(insert_photo_sql, newSite["pictures"])
                        cursor.executemany(insert_source_sql, newSite["sources"] )

                        # for pic in newSite["pictures"]:
                        #     try:
                        #         cursor.execute(insert_photo_sql, (site_id, pic["name"], pic["link"], pic["info"], datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                        #     except Exception as error:
                        #         self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database/Save Picture: %s', error)

                        # for source in newSite["sources"]:
                        #     try:
                        #         cursor.execute(insert_source_sql, (site_id, source["info"], datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                        #     except Exception as error:
                        #         self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database/Save Source: %s', error)

                        for siteType in newSite["types"]:
                            try:
                                cursor.execute(insert_site_with_type_sql, ( siteType, newSite["site_id"], datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                            except Exception as error:
                                self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database/Save Site Types: %s', error)




                    except Exception as error:
                        self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database: %s', error)
                after_insert = cursor.execute("SELECT COUNT() FROM manitobaHistoricalSite").fetchone()[0]
                print("Inserted " + str(after_insert - before_insert) + " new rows into manitobaHistoricalSite")





        except Exception as error:
            self.logger.errorint('DBOperations/manitoba_historical_website_save_data: %s', error)


if __name__ == "__main__":
    runStart = datetime.today()
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(filename="historical_society_scrapper.log",
                                                  maxBytes=10485760,
                                                  backupCount=10)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Database ")

    database = DBOperations()
    database.initialize_db()
    database.purge_data()

