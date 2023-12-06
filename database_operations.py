
from datetime import datetime
import logging
from dbcm import DBCM
import os
import glob
from os.path import abspath, dirname, join

class DBOperations:
    """Store and retrive site data"""

    logger = logging.getLogger("main." + __name__)

    def __init__(self):
        """Initializes varibles that will be used throught the class"""
        self.database = "historicalSiteData.db"


    def initialize_db(self):
        """Initializes the database"""
        with DBCM(self.database) as cursor:
            try:
                cursor.execute("""create table if not exists winnipegHistoricalSite
                (site_id INTEGER primary key autoincrement not null,
                name TEXT,
                streetName TEXT,
                streetNumber TEXT,
                constructionDate TEXT,
                shortUrl TEXT,
                longUrl TEXT,
                latitude REAL not null,
                longitude REAL not null,
                city TEXT,
                province TEXT,
                import_date TEXT
                );""")

                cursor.execute("""create table if not exists manitobaHistoricalSite
                (site_id INTEGER primary key not null,
                name TEXT,
                address TEXT,
                main_type TEXT,
                latitude REAL not null,
                longitude REAL not null,
                province TEXT,
                municipality TEXT,
                description TEXT,
                site_url TEXT,
                import_date TEXT
                );""")

                cursor.execute("""create table if not exists sitePhotos
                (photo_id INTEGER primary key autoincrement not null,
                site_id INTEGER not null,
                photo_name TEXT,
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

                cursor.execute("""create table if not exists siteType
                (siteType_id INTEGER primary key autoincrement not null,
                site_id INTEGER not null,
                type TEXT,
                import_date TEXT
                );""")
            except Exception as error:
                self.logger.error('DBOperations/initialize_db: %s', error)

    def purge_data(self):
      """Removes all data from the db"""
      with DBCM(self.database) as cursor:
          try:
              cursor.execute("""DELETE FROM winnipegHistoricalSite;""")
              cursor.execute("""DELETE FROM manitobaHistoricalSite;""")
              cursor.execute("""DELETE FROM sitePhotos;""")
              cursor.execute("""DELETE FROM siteSource;""")
              cursor.execute("""DELETE FROM siteType;""")

          except Exception as error:
              self.logger.error('DBOperations/purge_data: %s', error)



    def winnipeg_api_save_data(self, historical_sites_list):
        """Saves a dictionary of historical sites values from winnipeg open api to the database"""
        try:
            insert_sql =  """INSERT OR IGNORE into winnipegHistoricalSite
            ( name, streetName, streetNumber, constructionDate, shortUrl, longUrl, latitude, longitude,  city, province, import_date)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""




            with DBCM(self.database) as cursor:
                try:
                    before_insert = cursor.execute("SELECT COUNT() FROM winnipegHistoricalSite").fetchone()[0]
                    cursor.executemany(insert_sql, historical_sites_list)
                    after_insert = cursor.execute("SELECT COUNT() FROM winnipegHistoricalSite").fetchone()[0]
                    print("Inserted " + str(after_insert - before_insert) + " new rows")
                except Exception as error:
                    self.logger.error('DBOperations/winnipeg_api_save_data/Insert Into database: %s', error)
        except Exception as error:
            self.logger.errorint('DBOperations/save_data: %s', error)

    def manitoba_historical_website_save_data(self, historical_sites_list):
        """Saves the data from the Manitoba Historical Society"""
        try:
            #sql = """SELECT TOP 1 site_id FROM historicalSite WHERE streetName = ? AND streetNumber = ?"""

            insert_site_sql =  """INSERT OR IGNORE into manitobaHistoricalSite
            (site_id, name, address, main_type,  latitude, longitude, province, municipality, description, site_url, import_date)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

            insert_photo_sql =  """INSERT OR IGNORE into sitePhotos
            (site_id, photo_name, photo_url, info, import_date)
            values (?, ?, ?, ?, ?)"""

            insert_source_sql =  """INSERT OR IGNORE into siteSource
            (site_id, info, import_date)
            values (?, ?, ?)"""

            insert_type_sql =  """INSERT OR IGNORE into siteType
            (site_id, type, import_date)
            values (?, ?, ?)"""

            with DBCM(self.database) as cursor:
                print("Insert data from Manitoba Historical Society to database")
                before_insert = cursor.execute("SELECT COUNT() FROM manitobaHistoricalSite").fetchone()[0]
                for newSite in historical_sites_list:
                    try:
                        cursor.execute(insert_site_sql, ( newSite["site_id"], newSite["site_name"], newSite["address"], newSite["types"][0], newSite["latitude"], newSite["longitude"] , "MB" , newSite["municipality"], newSite["description"], newSite["url"] , datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
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
                                cursor.execute(insert_type_sql, (newSite["site_id"], siteType, datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                            except Exception as error:
                                self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database/Save Site Types: %s', error)




                    except Exception as error:
                        self.logger.error('DBOperations/manitoba_historical_website_save_data/Insert Into database: %s', error)
                after_insert = cursor.execute("SELECT COUNT() FROM manitobaHistoricalSite").fetchone()[0]
                print("Inserted " + str(after_insert - before_insert) + " new rows into manitobaHistoricalSite")





        except Exception as error:
            self.logger.errorint('DBOperations/manitoba_historical_website_save_data: %s', error)

