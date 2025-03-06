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
import aiohttp 
import asyncio
import csv



from database_operations import DBOperations

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())



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

  def get_all_sites_from_CSV(self):
      """Gets all sites"""
      sites_in_csv_file = []
      try:
        #self.save_image(self.noImageUrl, self.noImageUrl.split("/")[-1])
        with open ('sites_data.csv', mode='r') as site_csv_file:
            all_unprocessed_sites = csv.DictReader(site_csv_file)
            for unprocessed_site in all_unprocessed_sites:
              try:
                sites_in_csv_file.append(unprocessed_site)
              except Exception as error:
                self.logger.error("ManitobaHistoricalScrapper/get_all_sites_from_CSV/For each site dic: %s", error)
                self.errorCount += 1  
            
            #numOfSites = len(all_unprocessed_sites)
            """ try:
              async with asyncio.TaskGroup() as tg:
                for unprocessed_site in all_unprocessed_sites:
                  numOfSites += 1
                  tg.create_task(self.get_site_info_from_dic(unprocessed_site))
                  self.get_site_info_from_dic(unprocessed_site)

            except Exception as error:
                self.logger.error("ManitobaHistoricalScrapper/get_all_sites/For each site dic: %s", error)
                self.errorCount += 1
            print ('Total amount of sites: ' + str(numOfSites)) """


      except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_all_sites_from_CSV: %s", error)
            self.errorCount += 1
      return sites_in_csv_file
    
    
  async def get_info_for_all_sites(self, sites_to_fetch):
    """Runs the method get_site_info_from_dic for each site asynicly"""
    try:
      async with aiohttp.ClientSession() as session:
        try:
          tasks = []
          for site in sites_to_fetch:
            try:
              tasks.append(asyncio.create_task(self.get_site_info_from_dic(site, session)))
            
            except Exception as error:
              self.logger.error("ManitobaHistoricalScrapper/get_info_for_all_sites/append tasks: %s", error)
              self.errorCount += 1
          
          await asyncio.gather(*tasks)
        
        except Exception as error:
              self.logger.error("ManitobaHistoricalScrapper/get_info_for_all_sites/run tasks: %s", error)
              self.errorCount += 1
    
    except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_info_for_all_sites: %s", error)
            self.errorCount += 1
    

  async def get_site_info_from_dic(self, unprocessed_site, session):
     """Gets info from the dictionary, then gets info from html file"""

     errors = 0
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
          async with session.get(siteURL) as response:
            page =  await response.content.read()
            #page =  requests.get(siteURL)
            soup = BeautifulSoup(page, "html.parser")
            relevantData = soup.find_all("div", {"class": "content-container"})[0]
            #Android uses the text in HTML, IOS uses Markdown
            siteDescriptionHTML = ""
            siteDescriptionMarkdown = ""
            allP = relevantData.find_all("p")
            sitePictures = []
            siteSources = []
            siteTables = []
            imageStart = 0


            #Get Site description
            try:
              for p in allP:
                if 'Link to:' in p.text:
                  continue  
                
                
                #if p.findAll("img", recursive=False):
                if p.findAll("img"):
                  imageStart = allP.index(p)
                  break
                
                #Skip tables, we deal with them seperately 
                if p.parent.name == 'td':
                  continue
                
                
                
                
               
                textHtml = self.format_html_text(p, siteURL)
                textMarkdown = self.turn_html_into_markdown(p, siteURL)
                



                  #Some sites didn't close the p tag, so this should cut the text off in those senerios
                if( "\n\n" in textHtml):
                    textHtml = textHtml.split("\n\n")[0]
                    textMarkdown = textMarkdown.split("\n\n")[0]
                siteDescriptionHTML += textHtml + "\n\n"
                siteDescriptionMarkdown += textMarkdown + "\n\n"



            except Exception as error:
                firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Description: " + str(error)
                self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Description: %s \nUrl: " + siteURL + "\n", error)
                errors += 1



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
                if currentP.findAll("img"):
                    img = currentP.findAll("img")[0]

                    

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
                      if errors == 0:
                        firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Download Image: " + str(error)
                        self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Download Image:  %s \nUrl: " + siteURL + "\n", error)
                      errors += 1
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
                      
                  
                  
                  
                  sitePictures.append(( siteID, fileName, img_width, img_height, img_full_url, self.format_html_text(currentP, siteURL), self.turn_html_into_markdown(currentP, siteURL), datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
                  picLink = None
                  fileName = None
              except Exception as error:
                if errors == 0:
                  firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Image: " + str(error)
                  self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Image:  %s \nUrl: " + siteURL + "\n", error)
                errors += 1
              currentPicNum = currentPicNum + 1
              
              
              
            try:
              allTables = relevantData.find_all("table")
              for table in allTables:
                tableName = None
                tableContentHtml = ""
                tableContentMarkdown = ""
                
                
                rows = table.find_all("tr")
                
                #The last table on each page is a footer, where the first row also contains a table. If we encounter that, we know we are at the bottom of the page and can stop
              #  if rows[0].find("td").find_all("table"):
               #   break
                
               # if not rows[0].find_all("img"):
                #  break
                #Image tables only have one tr element. This is to filter them out
                if not rows[0].find_all("img") and len(rows) > 2:
                  #Trying to get the table title by using the h2 tag directly before the table
                  #Unfortuantly, the blockquote is surounded by "\n", so I am iterating through all the previous siblings till I find one that isn't "\n"
                  try:
                    blockquote = table.parent
                    titleTag = None
                    if blockquote != None:
                      for sibling in blockquote.previous_siblings:
                        if sibling != "\n":
                          titleTag = sibling
                          break
                      #If the first real tag after the table is h2, use that text to get the table title
                      if titleTag != None and titleTag.name == "h2":
                        tableName = titleTag.text
                  except Exception as error:
                    if errors == 0:
                      firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables/Get Table Name: " + str(error)
                      self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables/Get Table Name:  %s \nUrl: " + siteURL + "\n", error)
                    errors += 1
                  
                  #Turn Rows and Columns into one big string
                  try:
                    for currentRow in rows:
                      currentRowTextHtml = ""
                      currentRowTextMarkdown = ""
                      
                      for currentColumn in currentRow.find_all("td"):
                        
                        if currentRowTextHtml != "":
                          # " _@_ " is used to separate columns
                          currentRowTextHtml += " _@_ "
                          currentRowTextMarkdown += " _@_ "
                        
                        #Gets the column P  
                        columnP = currentColumn.findAll("p")[0]
                        
                        currentRowTextHtml += self.format_html_text(columnP, siteURL)
                        currentRowTextMarkdown += self.turn_html_into_markdown(columnP, siteURL)
                      
                      if tableContentHtml != "":
                        # " _%_ " is used to seperate rows
                        tableContentHtml += " _%_ "
                        tableContentMarkdown += " _%_ "
                      tableContentHtml += currentRowTextHtml
                      tableContentMarkdown += currentRowTextMarkdown
                                          
                  except Exception as error:
                    if errors == 0:
                      firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables/Get Table Content: " + str(error)
                      self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables/Get Table Content:  %s \nUrl: " + siteURL + "\n", error)
                    errors += 1
                  
                  siteTables.append(( siteID, tableName, tableContentHtml, tableContentMarkdown, datetime.today().strftime('%Y-%m-%d %H:%M:%S')))

            except Exception as error:
                if errors == 0:
                  firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables: " + str(error)
                  self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse Site Tables:  %s \nUrl: " + siteURL + "\n", error)
                errors += 1 

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

                    siteSources.append(( siteID, self.format_html_text(currentSource, siteURL), self.turn_html_into_markdown(currentSource, siteURL),  datetime.today().strftime('%Y-%m-%d %H:%M:%S')))



                    #Get next source
                    currentSource = currentSource.find_next_sibling('p')

                  except Exception as error:
                    if errors == 0:
                      firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse through Sources: " + str(error)
                      self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse through Sources: %s \nUrl: " + siteURL + "\n", error)
                    errors += 1
            except Exception as error:
              if errors == 0:
                firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Get Sources: " + str(error)
                self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Get Sources: %s \nUrl: " + siteURL + "\n", error)
              errors += 1


            #If there were no errors, add to allSites
            if errors == 0:
              self.allSites.append(dict(site_id = siteID, site_name = siteName, types = siteTypes, municipality =  siteMuni, address = siteAddress, latitude = siteLatitude, longitude = siteLongitude, descriptionHTML = siteDescriptionHTML, descriptionMarkdown = siteDescriptionMarkdown, pictures  = sitePictures, sources = siteSources, tables = siteTables, keywords = siteKeywords, url = siteURL))
        except Exception as error:
          if errors == 0:
            firstErrorMessage = "ManitobaHistoricalScrapper/get_site_info_from_dic/Parse_Site_Webpage: " + str(error)
            self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic/Parse_Site_Webpage: %s", error)
          errors += 1


        #If there was an error, add to bad sites
        if errors > 0:
          self.badSites.append(dict(name = siteName, municipality = siteMuni, address = siteAddress, url = siteURL, error = firstErrorMessage))
     except Exception as error:
            self.logger.error("ManitobaHistoricalScrapper/get_site_info_from_dic: %s", error)
            errors += 1
     self.errorCount += errors
     
     
  def format_urls_in_text(self, url):
    """Turns the urls from relative links to not that"""
    linkText = url

    #For info links to other websites
    linkText = linkText.replace('../../', self.baseUrl)

    #Municipalities or people
    linkText = linkText.replace('../', self.baseUrlWithDocs)
    
    if linkText[:6] == '/docs/' or linkText[:6] == '/info/':
      linkText = self.baseUrl + linkText[1:]
      

    #Sites are linked by html file only
    if "mailto:webmaster@mhs.mb.ca" not in linkText and "/" not in linkText and ".shtml" in linkText:
      linkText = self.baseUrlForSite + linkText
    
    return linkText
    

  def format_html_text(self, p, siteURL):
     """Gets p and returns text with the links embedded"""
     returnText = ""
     try:

      for line in p.contents:
        try:
           if '<a href=' in str(line):
              if line.name != 'a':
                line = line.find_all('a', href=True)[0]                
              line["href"] = self.format_urls_in_text(line["href"])
           returnText += str(line) #.replace("<br/>", " \n")
        except Exception as error:
          self.logger.error("ManitobaHistoricalScrapper/get_text_with_links/parce_through_contents: %s \nUrl: %s", error, siteURL)
          self.errorCount += 1


     except Exception as error:
        self.logger.error("ManitobaHistoricalScrapper/get_text_with_links: %s \nUrl: %s", error, siteURL)
        self.errorCount += 1
     return returnText
   
  def turn_html_into_markdown(self, p, siteURL):
    """Gets p and returns text in markdown, needed because IOS uses markdown""" 
    returnText = ""
    try:
      for line in p.contents:
        try:
           markdownText = "" + str(line.string or '')
           if '<a href=' in str(line):
              if line.name != 'a':
                line = line.find_all('a', href=True)[0]                
              if line.string is not None:
                markdownText = "[" + line.string + "](" + self.format_urls_in_text(line["href"]) +")"
           
           if line.name == "strong" or line.name == "b":
             markdownText = "**" + markdownText + "**"
           
           if line.name == "em" or line.name == "i":
             markdownText = "*" + markdownText + "*"
           
           if line.name == "br" or line.name == "p" or line.name == "div":
             markdownText = "\n"
             
           returnText += markdownText #.replace("<br/>", " \n")
        except Exception as error:
          self.logger.error("ManitobaHistoricalScrapper/turn_html_into_markdown/parce_through_contents: %s \nUrl: %s", error, siteURL)
          self.errorCount += 1
    except Exception as error:
      self.logger.error("ManitobaHistoricalScrapper/turn_html_into_markdown: %s \nUrl: %s", error, siteURL)
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
                , "file":"williamwhyteschool.shtml"
                , "lat":"51.14021"
                , "lng":"-100.03943"}
    
    testSiteList = [testSite]
    
   
    sitesInCSV = siteScraper.get_all_sites_from_CSV()
    
    numOfSitesInCSV = len(sitesInCSV)
    
    print(str(numOfSitesInCSV) + " sites in the csv file ")
    
    #asyncio.run(siteScraper.get_info_for_all_sites(testSiteList))
    asyncio.run(siteScraper.get_info_for_all_sites(sitesInCSV))


    endTime = datetime.today()
    
    numOfSitesCollected = len(siteScraper.allSites)
    print("# of sites successfully collected: " + str(numOfSitesCollected))
    print("# of error fetching data: " + str(siteScraper.errorCount))
    print("# of bad sites " + str(len(siteScraper.badSites)))
    print(str(round((numOfSitesCollected/numOfSitesInCSV) * 100, 5)) + " percent of sites successfully collected!")
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






