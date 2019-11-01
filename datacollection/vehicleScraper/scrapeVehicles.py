import os
from json import loads
import json
from lxml import html
from datetime import datetime
from requests_html import HTMLSession
import boto3

def runScraper():

    # connect to s3, download cities file    
    s3_session = boto3.Session(profile_name='omkar')
    s3 = s3_session.resource('s3')
    bucket = s3.Bucket('citiesscraped')
    bucket.download_file('cities.txt', 'cities.txt')
    
    citiesList = []
    with open('cities.txt') as json_file:
        cities = json.load(json_file)
        for key, value in cities.items():
            citiesList.append(value['url'])  

    citiesCount = len(citiesList)
    session = HTMLSession()   
    scraped = 0
    cities = 0
    nextYear = datetime.now().year + 1
    carBrands = ["ford", "toyota", "chevrolet", "chev", "chevy", "honda", "jeep", "hyundai", "subaru",
                 "kia", "gmc", "ram", "dodge", "mercedes-benz", "mercedes", "mercedesbenz",
                 "volkswagen", "vw", "bmw", "saturn", "land rover", "landrover", "pontiac", 
                 "mitsubishi", "lincoln", "volvo", "mercury", "harley-davidson", "harley", 
                 "rover", "buick", "cadillac", "infiniti", "infinity", "audi", "mazda", "chrysler",
                 "acura", "lexus", "nissan", "datsun", "jaguar", "alfa", "alfa-romeo", "aston", "aston-martin",
                 "ferrari", "fiat", "hennessey", "porche", "noble", "morgan", "mini"]
    

    for city in citiesList:
        scrapedInCity = 0
        cities += 1
        print(f"Scraping vehicles from {city}, {citiesCount - cities} cities remain")
        empty = False
        
        #scrapedIds is used to store each individual vehicle id from a city, therefore we can delete vehicle records from the database
        #if their id is no longer in scrapedIds under the assumption that the entry has been removed from craigslist
        scrapedIds = set([]) 
        
        #track items skipped that are already in the database
        skipped = 0
        
        #this loop executes until we are out of search results, craigslist sets this limit at 3000 and cities often contain the full 3000 records (but not always)        
        while not empty:
            print(f"Gathering entries {scrapedInCity} through {scrapedInCity + 120}")
            
            #now we scrape
            try:
                searchUrl = f"{city}/d/cars-trucks/search/cta?s={scrapedInCity}"
                page = session.get(searchUrl)
            except Exception as e:
                #catch any excpetion and continue the loop if we cannot access a site for whatever reason
                print(f"Failed to reach {searchUrl}, entries have been dropped: {e}")
                continue
            
            #each search page contains 120 entries
            scrapedInCity += 120
            tree = html.fromstring(page.content)
            
            #the following line returns a list of urls for different vehicles
            vehicles = tree.xpath('//a[@class="result-image gallery"]')
            if len(vehicles) == 0:
                #if we no longer have entries, continue to the next city
                empty = True
                continue
            vehiclesList = []
            for item in vehicles:
                vehicleDetails = []
                vehicleDetails.append(item.attrib["href"])
                try:
                    #this code attempts to grab the price of the vehicle. some vehicles dont have prices (which throws an exception)
                    #and we dont want those which is why we toss them
                    vehicleDetails.append(item[0].text)
                except:
                    continue
                vehiclesList.append(vehicleDetails)
            
            #loop through each vehicle
            for item in vehiclesList:
                url = item[0]
                try:
                    idpk = int(url.split("/")[-1].strip(".html"))
                except ValueError as e:
                    print("{} does not have a valid id: {}".format(url, e))
                
                #add the id to scrapedIds for database cleaning purposes
                scrapedIds.add(idpk)
                
                #vehicle id is a primary key in this database so we cant have repeats. if a record with the same url is found, we continue
                #the loop as the vehicle has already been stored                
                
                vehicleDict = {}
                vehicleDict["price"] = int(item[1].strip("$"))
                
                try:
                    #grab each individual vehicle page
                    page = session.get(url)
                    tree = html.fromstring(page.content)
                except:
                    print(f"Failed to reach {url}, entry has been dropped")
                    continue
                
                attrs = tree.xpath('//span//b')
                #this fetches a list of attributes about a given vehicle. each vehicle does not have every specific attribute listed on craigslist
                #so this code gets a little messy as we need to handle errors if a car does not have the attribute we're looking for
                for item in attrs:
                    try:
                        #model is the only attribute without a specific tag on craigslist, so if this code fails it means that we've grabbed the model of the vehicle
                        k = item.getparent().text.strip()
                        k = k.strip(":")
                    except:
                        k = "model"
                    try:
                        #this code fails if item=None so we have to handle it appropriately
                        vehicleDict[k] = item.text.strip()
                    except:
                        continue
                    
                #we will assume that each of these variables are None until we hear otherwise
                #that way, try/except clauses can simply pass and leave these values as None
                price = None
                year = None
                manufacturer = None
                model = None
                condition = None
                cylinders = None
                fuel = None
                odometer = None
                title_status = None
                transmission = None
                VIN = None
                drive = None
                size = None
                vehicle_type = None
                paint_color = None
                image_url = None
                lat = None
                long = None
                description = None
                
                #now this code gets redundant. if we picked up a specific attr in the vehicleDict then we can change the variable from None.
                #integer attributes (price/odometer) are handled in case the int() is unsuccessful, but i have never seen that be the case
                if "price" in vehicleDict:
                    try:
                        price = int(vehicleDict["price"])
                    except Exception as e:
                        print(f"Could not parse price: {e}")
                if "odomoter" in vehicleDict:
                    try:
                        odometer = int(vehicleDict["odometer"])
                    except Exception as e:
                        print(f"Could not parse odometer: {e}")
                if "condition" in vehicleDict:
                    condition = vehicleDict["condition"]
                if "model" in vehicleDict:
                    #model actually contains 3 variables that we'd like: year, manufacturer, and model (which we call model)
                    try:
                        year = int(vehicleDict["model"][:4])
                        if year > nextYear:
                            year = None
                    except:
                        year = None
                    model = vehicleDict["model"][5:]
                    foundManufacturer = False
                    #we parse through each word in the description and search for a match with carBrands (at the top of the program)
                    #if a match is found then we have our manufacturer, otherwise we set model to the entire string and leave manu blank
                    for word in model.split():
                        if word.lower() in carBrands:
                            foundManufacturer = True
                            model = ""
                            #resolve conflicting manufacturer titles
                            manufacturer = word.lower()
                            if manufacturer == "chev" or manufacturer == "chevy":
                                manufacturer = "chevrolet"
                            if manufacturer == "mercedes" or manufacturer == "mercedesbenz":
                                manufacturer = "mercedes-benz"
                            if manufacturer == "vw":
                                manufacturer = "volkswagen"
                            if manufacturer == "landrover":
                                manufacturer = "land rover"
                            if manufacturer == "harley":
                                manufacturer = "harley-davidson"
                            if manufacturer == "infinity":
                                manufacturer = "infiniti"
                            if manufacturer == "alfa":
                                manufacturer = "alfa-romeo"
                            if manufacturer == "aston":
                                manufacturer = "aston-martin"
                            continue
                        if foundManufacturer:
                            model = model + word.lower() + " "
                    model = model.strip()
                if "cylinders" in vehicleDict:
                    cylinders = vehicleDict["cylinders"]
                if "fuel" in vehicleDict:
                    fuel = vehicleDict["fuel"]
                if "odometer" in vehicleDict:
                    odometer = vehicleDict["odometer"]
                if "title status" in vehicleDict:
                    title_status = vehicleDict["title status"]    
                if "transmission" in vehicleDict:
                    transmission = vehicleDict["transmission"]
                if "VIN" in vehicleDict:
                    VIN = vehicleDict["VIN"]
                if "drive" in vehicleDict:
                    drive = vehicleDict["drive"]
                if "size" in vehicleDict:
                    size = vehicleDict["size"]
                if "type" in vehicleDict:
                    vehicle_type = vehicleDict["type"]
                if "paint color" in vehicleDict:
                    paint_color = vehicleDict["paint color"]
                    
                #now lets fetch the image url if exists
                
                try:
                    img = tree.xpath('//div[@class="slide first visible"]//img')
                    image_url = img[0].attrib["src"]
                except:
                    pass
                
                #try to fetch lat/long and city/state, remain as None if they do not exist
                
                try:
                    location = tree.xpath("//div[@id='map']")
                    lat = float(location[0].attrib["data-latitude"])
                    long = float(location[0].attrib["data-longitude"])
                except:
                    pass
                
                #try to fetch a vehicle description, remain as None if it does not exist
                
                try:
                    location = tree.xpath("//section[@id='postingbody']")
                    description = location[0].text_content().strip()
                    description = description.rstrip()
                    description = description.lstrip()
                    description = description.strip("QR Code Link to This Post")
                    description = description.lstrip()
                except:
                    pass
                    
                
                #finally we get to insert the entry into the database
                scraped += 1
            #these lines will execute every time we grab a new page (after 120 entries)
            print("{} vehicles scraped".format(scraped))
            print(vehicleDict) 
          
def main():
    runScraper()


if __name__ == "__main__":
    main()
