import logging
import logging.handlers
from fetch_site_data import FetchSiteDate

class MainProcessor:
  """Main class that calls others based on user input"""

  logger = logging.getLogger("main." + __name__)

  def __init__(self):
      """Initializes the classes that will be used in the application"""
      try:
          self.main_menu = { 1: "Fetch Data from City of Winnipeg",

                          5: "Exit"}
          self.fetch_from_URL = FetchSiteDate()
      except Exception as error:
          self.logger.error("MainProcessor/init: %s", error)
          self.fetch_from_URL = None

