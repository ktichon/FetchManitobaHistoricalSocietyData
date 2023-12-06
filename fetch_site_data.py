import urllib.request, json
import logging
from database_operations import DBOperations
from datetime import datetime

class FetchSiteDate:
    """Class that fetchs data from various sources for my app"""

    logger = logging.getLogger("main." + __name__)

    def __init__(self):
        self.jsonDataDictionary = {}
        self.currentSiteId = 0

    def fetch_from_winnipeg_api(self):
        """Fetches data from the winnipeg open data"""
        processedData = []
        try:
            with urllib.request.urlopen("https://data.winnipeg.ca/resource/ptpx-kgiu.json?") as url:
                self.jsonDataDictionary = json.load(url)


            for value in self.jsonDataDictionary:
                """Name, street name, street number, constructed date, short url, long url, latitude, longitude, city, provance """
                try:
                    processedData.append( (self.ifInValue("historical_name", value), self.ifInValue("street_name", value), self.ifInValue("street_number", value), self.ifInValue("construction_date", value), self.ifInValue("short_report_url", value), self.ifInValue("long_report_url", value), self.ifInValue("latitude", self.ifInValue("location", value)), self.ifInValue("longitude", self.ifInValue("location", value)),  "Winnipeg", "MB", datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                except Exception as error:
                    self.logger.error("fetch_from_winnipeg_api/append data to list %s", error)
        except Exception as error:
                    self.logger.error("fetch_from_winnipeg_api %s", error)
        return processedData
    def ifInValue (self, string, value):
        returnValue = None
        try:
            if value is not None and string in value:
                return value[string]
        except Exception as error:
             self.logger.error("ifInValue %s", error)
             returnValue = None
        return returnValue



if __name__ == "__main__":
    fetchData = FetchSiteDate()
    processedData = fetchData.fetch_from_winnipeg_api()
    fetchData.fetch_from_winnipeg_api()
    print(fetchData.jsonDataDictionary[79])
    print(fetchData.jsonDataDictionary[79]["location"]["latitude"])
    print(fetchData.jsonDataDictionary[79]["historical_name"])
    print(len(fetchData.jsonDataDictionary))
    print(len(processedData))

    database = DBOperations()
    database.initialize_db()
    database.purge_data()
    database.winnipeg_api_save_data(processedData)





