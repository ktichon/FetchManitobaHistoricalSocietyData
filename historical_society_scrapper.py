"""
Scrapes Historical Site data base on url
"""
from html.parser import HTMLParser
import logging
import logging.handlers
import re
#import datetime
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from os.path import abspath, dirname, join
import calendar
import time
import asyncio
import csv



from database_operations import DBOperations



class ManitobaHistoricalScrapper():
  """Scrapes info from """
  logger = logging.getLogger("main." + __name__)

  baseSiteImageUrl = "http://www.mhs.mb.ca/docs/sites/images/"

  baseUrl = "http://www.mhs.mb.ca/"
  baseUrlWithDocs = "http://www.mhs.mb.ca/docs/"

  baseUrlForSite = "http://www.mhs.mb.ca/docs/sites/"
  #Urls that are not included because they are not a phyiscal site

  noImageUrl = "http://www.mhs.mb.ca/docs/sites/images/nophoto.jpg"

  def __init__(self):
    try:

        self.allSites = []
        self.saveImages = True
        self.errorCount = 0
        self.badSites = []


    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/init: %s", error)

  def save_image(self, imageUrl, fileName):
    """Saves the image to the Site_Images folder"""
    try:
      if self.saveImages:
        img_data = requests.get(imageUrl).content
        imagePath = join(dirname(abspath(__file__)), "Site_Images", fileName)
        with open(imagePath, 'wb') as handler:
          handler.write(img_data)
    except Exception as error:
        self.logger.error("ManitobaHistoricalScrapper/save_image: %s", error)
        self.errorCount += 1

  def get_all_sites(self):
      """Gets all sites"""

      try:
        self.save_image(self.noImageUrl, self.noImageUrl.split("/")[-1])
        with open ('sites_data.csv', mode='r', encoding='utf-8-sig') as site_csv_file:
            all_unprocessed_sites = csv.DictReader(site_csv_file)
            numOfSites = 0

            try:
              for unprocessed_site in all_unprocessed_sites:
                numOfSites += 1
                self.get_site_info_from_dic(unprocessed_site)

            except Exception as error:
                self.logger.error("ManitobaHistoricalScrapper/get_all_sites/For each site dic: %s", error)
                self.errorCount += 1
            print ('Total amount of sites: ' + str(numOfSites))


      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_sites: %s", error)
            self.errorCount += 1

  def get_site_info_from_dic(self, unprocessed_site):
     """Gets info from the dictionary, then gets info from html file"""

     startError = self.errorCount
     firstErrorMessage = ""

     try:
        siteName = unprocessed_site["site"]
        siteID = unprocessed_site["num"]
        siteTypes =list(unprocessed_site["sitetype"].split(','))
        siteMuni = unprocessed_site["describe"].replace("`", "Other")
        streetName = unprocessed_site["location"]
        streetNumber = unprocessed_site["number"]

        #Set street address
        siteAddress = streetNumber
        if(streetNumber):
           siteAddress += " "
        siteAddress += streetName

        #siteKeywords = list(unprocessed_site["keyword"].split(','))
        #Probably best to keep in on string to make it easyer to search for a site (in the front end)
        siteKeywords = unprocessed_site["keyword"]
        siteLatitude = unprocessed_site["lat"]
        siteLongitude = unprocessed_site["lng"]
        siteFile = unprocessed_site["file"]
        siteURL = self.baseUrlForSite + siteFile
        
        #if siteName == "Holy Trinity Roman Catholic Cemetery":
          #print("Found You!")
          
        
        


        #Gets site info from link


        try:
          
          #If the site doesn't have a Latitude or Longitude, don't bother with it
          if siteLatitude is None or siteLatitude == '' or siteLatitude == 0  or siteLongitude is None or siteLongitude == '' or siteLongitude == 0:
            raise Exception("Invaild Site, missing coordinates information")
          
          #If the site doen't have a link, don't waste time on it
          if not siteFile:
           raise Exception("Invaild Site, missing key infomation")
          page = requests.get(siteURL)
          soup = BeautifulSoup(page.content, "html.parser")
          relevantData = soup.find_all("div", {"class": "content-container"})[0]
          siteDescription = ""
          allP = relevantData.find_all("p")
          sitePictures = []
          siteSources = []
          imageStart = 0


          #Get Site description
          try:
            for p in allP:
              if 'Link to:' in p.text:
                 continue    
              if p.findAll("img", recursive=False):
                 imageStart = allP.index(p)
                 break;

              text = self.get_text_with_links(p, siteURL)



                #Some sites didn't close the p tag, so this should cut the text off in those senerios
              if( "\n\n" in text):
                  text = text.split("\n\n")[0]
              siteDescription += text + ("<br><br>")



          except Exception as error:
              firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Description: " + str(error)
              self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Description: %s \nUrl: " + siteURL + "\n", error)
              self.errorCount += 1



          #Getting site pictures

          picLink = None
          fileName = None
          img_full_url = None
          img_width = 600
          img_height = 450


          currentPicNum = imageStart


          while currentPicNum < len(allP):
            try:
              currentP = allP[currentPicNum]
              if "Site" in currentP.text and "(lat/long):" in currentP.text:
                break
              if currentP.findAll("img", recursive=False):
                  img = currentP.findAll("img", recursive=False)[0]

                  

                  picLink = img['src']
                  try:
                      picName = picLink.split("/")[-1]
                      if "../" in picLink:
                          websitePath = picLink.split("../")[1]
                          img_full_url = self.baseUrlWithDocs + websitePath
                      else:
                        img_full_url = self.baseSiteImageUrl + picName
                        
                      #If no image, dont save it.
                      if img_full_url == self.noImageUrl:
                        picLink = None
                        fileName = None
                        break
                      else:
                        #If photo has specified dimensions, get them
                        if img.has_attr('width'):
                          img_width = img['width']
                        if img.has_attr('height'):
                          img_height = img['height']
                        fileName = str(siteID) + "_" + picName.split(".")[0] + "_" + str(calendar.timegm(time.gmtime())) + "." + picName.split(".")[1]
                        self.save_image(img_full_url, fileName)

                      #Added logic so that it only downloads a new image if it isn't the "nophoto" image
                      """ if img_full_url != self.noImageUrl:
                        #firstPartOfName = (siteURL.split("/")[-1]).replace("shtml-", "")
                        fileName = str(siteID) + "_" + picName.split(".")[0] + "_" + str(calendar.timegm(time.gmtime())) + "." + picName.split(".")[1]
                        self.save_image(img_full_url, fileName)
                      else:
                        fileName = self.noImageUrl.split("/")[-1] """
                  except Exception as error:
                    if startError == self.errorCount:
                      firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Download Image: " + str(error)
                      self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Download Image:  %s \nUrl: " + siteURL + "\n", error)
                    self.errorCount += 1
              elif picLink != None and currentP.text != '\n':
                #Relized that this was not necessarily, as I could just sort by photo_id in the app.
                #  year = 0
                #try:
                #  yearBracket = currentP.text[currentP.text.find('(') + 1 : currentP.text.find(')')]
                #  yearMach =  re.search(r'\d{4}', yearBracket)
                #  if yearMach == None:
                #    year = 0
                #  else:
                #    year = int(yearMach.string)
                #except Exception as error:
                #    if startError == self.errorCount:
                #      firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Get Image Year: " + str(error)
                #      self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Get Image Year:  %s \nUrl: " + siteURL + "\n", error)
                #    year = 0
                #    self.errorCount += 1 
                    
                
                
                
                sitePictures.append(( siteID, fileName, img_width, img_height, img_full_url, self.get_text_with_links(currentP, siteURL), datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                picLink = None
                fileName = None
            except Exception as error:
              if startError == self.errorCount:
                firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Image: " + str(error)
                self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Image:  %s \nUrl: " + siteURL + "\n", error)
              self.errorCount += 1
            currentPicNum = currentPicNum + 1

          try:
            sourceStart = relevantData.find(id="sources")
            if sourceStart == None:
              sourceStart = relevantData.find("h2", string="Sources:" )
            currentSource = sourceStart.find_next_sibling('p')

            for loop in range(50):
                try:

                  #If end of sources, exit loop
                  if currentSource == None or "Page revised: " in currentSource.text:
                    break

                  siteSources.append(( siteID, self.get_text_with_links(currentSource, siteURL),  datetime.today().strftime('%Y-%m-%d %H:%M:%S')))



                  #Get next source
                  currentSource = currentSource.find_next_sibling('p')

                except Exception as error:
                  if startError == self.errorCount:
                    firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse through Sources: " + str(error)
                    self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse through Sources: %s \nUrl: " + siteURL + "\n", error)
                  self.errorCount += 1
          except Exception as error:
            if startError == self.errorCount:
              firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Get Sources: " + str(error)
              self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Get Sources: %s \nUrl: " + siteURL + "\n", error)
            self.errorCount += 1


          #If there were no errors, add to allSites
          if self.errorCount == startError:
             self.allSites.append(dict(site_id = siteID, site_name = siteName, types = siteTypes, municipality =  siteMuni, address = siteAddress, latitude = siteLatitude, longitude = siteLongitude, description = siteDescription, pictures  = sitePictures, sources = siteSources, keywords = siteKeywords, url = siteURL))
        except Exception as error:
          if startError == self.errorCount:
            firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse_Site_Webpage: " + str(error)
            self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse_Site_Webpage: %s", error)
          self.errorCount += 1


        #If there was an error, add to bad sites
        if self.errorCount > startError:
          self.badSites.append(dict(name = siteName, municipality = siteMuni, address = siteAddress, url = siteURL, error = firstErrorMessage))
     except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic: %s", error)
            self.errorCount += 1

  def get_text_with_links(self, p, siteURL):
     """Gets p and returns text with the links embedded"""
     returnText = ""
     try:

      for line in p.contents:
        try:
           if '<a href=' in str(line):
              if line.name != 'a':
                line = line.find_all('a', href=True)[0]

              linkText = line["href"]

              #For info links to other websites
              linkText = linkText.replace('../../', self.baseUrl)

              #Municipalities or people
              linkText = linkText.replace('../', self.baseUrlWithDocs)
              test = linkText[:6]
              test2 = linkText[1:]
              
              if linkText[:6] == '/docs/' or linkText[:6] == '/info/':
                linkText = self.baseUrl + linkText[1:]
                

              #Sites are linked by html file only
              if "mailto:webmaster@mhs.mb.ca" not in linkText and "/" not in linkText and ".shtml" in linkText:
                linkText = self.baseUrlForSite + linkText

              #Made the links not relative
              line["href"] = linkText
           returnText += str(line) #.replace("<br/>", " \n")
        except Exception as error:
          self.logger.error("ManitobaHistoricalScrapper/get_text_with_links/parce_through_contents: %s \nUrl: %s", error, siteURL)
          self.errorCount += 1


     except Exception as error:
        self.logger.error("ManitobaHistoricalScrapper/get_text_with_links: %s \nUrl: %s", error, siteURL)
        self.errorCount += 1
     return returnText
  def log_bad_sites(self):
     """Writes all invalid sites to a txt file"""
     try:
      file = open("Invalid_Sites.txt", "w")
      for badSite in self.badSites:
          siteLine = str(self.badSites.index(badSite) + 1) +  ") " + badSite["name"] + ", " + badSite["municipality"] + ", "  + badSite["address"] + ", "  + badSite["url"] + "\n" + badSite["error"] + "\n\n"
          file.write(siteLine)

     except Exception as error:
              self.logger.error("ManitobaHistoricalScrapper/log_bad_sites: %s", error)
















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
    logger.info("Application Historical Society Scrapper Started")
    startTime = datetime.today()
    siteScraper = ManitobaHistoricalScrapper()
    siteScraper.saveImages = False


    print("Start fetching data at " + str(startTime))

    testSite = {"site": "Brown Barn"
                , "num": "611"
                , "sitetype": "6"
                , "describe": "Dauphin (RM)"
                , "location":""
                , "number": ""
                , "keyword":"approx, Cooperator, photo=1981"
                , "file":"roblinparkcommunitycentre.shtml"
                , "lat":"51.14021"
                , "lng":"-100.03943"}

    #siteScraper.get_site_info_from_dic(testSite)

    siteScraper.get_all_sites()


    endTime = datetime.today()

    print("# of error fetching data: " + str(siteScraper.errorCount))
    print("# of bad sites " + str(len(siteScraper.badSites)))
    print("Completed fetching data at " + str(endTime))
    print("Time it took to fetch data: " + str(endTime - startTime))
    print("Logging bad sites")
    siteScraper.log_bad_sites()



    logger.info("Insert Data into Database")
    startTime = datetime.today()
    print("Started data operations at " + str(startTime))

    database = DBOperations()
    database.initialize_db()
    database.purge_data()
    database.manitoba_historical_website_save_data(siteScraper.allSites)
    endTime = datetime.today()
    print("Completed data operations at " + str(endTime))
    print("Time it took to complete data operations : " + str(endTime - startTime))
    print("Total run time " + str(datetime.today() - runStart))






