#!/usr/bin/env python

import argparse
import boto3
from datetime import datetime
import json
import kafkaconnect
import logging
from lxml import html
from collections import OrderedDict
import os
from queue import Queue
from requests_html import HTMLSession
import threading

cityQueue = Queue.Queue()

for i in range(4):
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

def runScraper():

	# connect to s3, download cities file	 
	s3_session = boto3.Session(profile_name='omkar')
	s3 = s3_session.resource('s3')
	bucket = s3.Bucket('citiesscraped')
	bucket.download_file('cities.txt', 'cities.txt')
	
	# connect to kafka
	parser = argparse.ArgumentParser()
	parser.add_argument("--hosts", required=True, default="localhost:9092")
	parser.add_argument("--topics", required=True, default="vehicleScraper")
	argument = parser.parse_args()
	hosts = str(argument.hosts)
	topics = str(argument.topics)
	
	client, producer = kafkaconnect.connect(hosts, topics)

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
	
	carBrands = ['acura', 'alfa', 'alfa romeo', 'alfa-romeo', 'alpina', 'anteros', 'aston', 'aston martin', 
				'aston-martin', 'astonmartin', 'audi', 'aurica', 'benz', 'bimmer', 'bmw', 'bugatti', 'buick', 'bxr', 'cadillac', 'chev', 
				'chevrolet', 'chevy', 'chrysler', 'datsun', 'desoto', 'dodge', 'eagle', 'elio motors', 'falcon', 
				'faraday', 'ferrari', 'fiat', 'fisker', 'ford', 'ford motors', 'ford-motors', 'gm', 'gmc', 'harley', 'harley davidson', 
				'harley-davidson', 'hennessey', 'honda', 'hyundai', 'infiniti', 'infinity', 'jaguar', 'jeep', 'karma', 
				'kia', 'lambo', 'lamborghini', 'land rover', 'landrover', 'lexus', 'lincoln', 'local', 'lotus', 'lucid', 
				'lyons', 'magnum', 'maserati', 'mazda', 'mazzanti', 'mb', 'mclaren', 'mercedes', 'mercedes benz', 'mercedes-benz',
				'mercedesbenz', 'mercury', 'mini', 'mitsubishi', 'morgan', 'niama', 'niama-reisser', 'nissan', 'noble', 
				'oldsmobile', 'pagani', 'panoz', 'plymouth', 'polaris', 'pontiac', 'porsche', 'racefab', 'ram', 'ram trucks', 
				'rezvani', 'rivian', 'rossion', 'rover', 'saleen', 'saturn', 'shelby', 'sofia', 'spyker', 'ssc', 'studebaker', 
				'subaru', 'tesla', 'toyota', 'trion', 'tvr', 'valmet', 'volkswagen', 'volvo', 'vw', 'zimmer']	

	aliasDict = {
			# alfa romeo
			"alfa":"alfa romeo",
			"alfa-romeo":"alfa romeo",
			"romeo":"alfa romeo",
			# aston martin
			"aston-martin":"aston martin",
			"aston":"aston martin",
			"astonmartin":"aston martin",
			# bmw
			"bimmer":"bmw",
			# chevy
			"chevy":"chevorlet",
			"chev":"chevorlet",
			# ford
			"ford motors":"ford",
			"fordmotors": "ford",
			# harley davidson
			"harley davidson":"harley-davidson",
			"harley":"harley-davidson",
			# infiniti
			"infinity":"infiniti",
			# lamborghini
			"lambo":"lamborghini",
			# land rover
			"land-rover":"land rover",
			"landrover" :"land rover",
			"rover":"land rover",
			# mercedes
			"benz":"mercedes-benz",
			"mercedes":"mercedes-benz",
			"mercedes benz":"mercedes-benz",
			"mb":"mercedes-benz",
			"mercedesbenz":"mercedes-benz",
			# volkswagen
			"vw":"volkswagen" 
		}

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
				vehicleDict["city"] = city
				try:
					#grab each individual vehicle page
					page = session.get(url)
					tree = html.fromstring(page.content)
				except:
					print(f"Failed to reach {url}, entry has been dropped")
					continue
			   
				# get vehicles from today, otherwise skip
				date = tree.xpath("//time[@class='date timeago']")
				
				# page could be under review or listing might have been removed, in that case skip
				if (date == []): continue 

				dt = date[0].attrib['datetime']

				# filter date by today's date WARNING (run only from 9AM-5PM UTC))
				#print(datetime.utcnow().strftime('%Y-%m-%d'))

				#if dt.split('T')[0] != datetime.utcnow().strftime('%Y-%m-%d'):
					#continue

				attrs = tree.xpath('//span//b')
				#this fetches a list of attributes about a given vehicle. each vehicle does not have every specific attribute listed on craigslist
				#so this code gets a little messy as we need to handle errors if a car does not have the attribute we're looking for
				for item in attrs:
					try:
						# model is the only attribute without a specific tag on craigslist
						# if this code fails it means that we've grabbed the model of the vehicle
						k = item.getparent().text.strip()
						k = k.strip(":")
					except:
						k = "model_raw"
					try:
						#this code fails if item=None so we have to handle it appropriately
						vehicleDict[k] = item.text.strip()
					except:
						continue
					
				#we will assume that each of these variables are None until we hear otherwise
				#that way, try/except clauses can simply pass and leave these values as None
				price = None
				year = None
				make = None
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
								
				# TODO: find a standardized way to store make and model (hacky solution rn)
				if "model_raw" in vehicleDict:
					foundYear = False
					foundMake = False
					duplicateYear = False					
					idxYear = None
					idxMake = None
					modelWords = model.split(" ")

					# assuming year is the first word (99%+ case)
					if len(modelWords[0]) == 4 and modelWords[0].isnumeric():
						foundYear = True
						year = int(modelWords[0])
						del modelWords[0]
					
					# duplicate year
					if str(year) == modelWords[0]:
						del modelWords[0]:
			
					if len(modelWords) == 1 and modelWords[0].lower() in carBrands:
						
					
					
					for idx, word in enumerate(modelWords):
						if word.lower() in carBrands:
							make = word.lower()
							if idx+1 > len(modelWords)-1:
								if modelWords[idx].lower() + modelWords[idx+1].lower() in carBrands:	
									
					###
	
					for idx, word in enumerate(modelWords):
						if len(word) == 4 and word.isnumeric():
							if foundYear and :
								duplicateYear = idx
								continue

							year = int(word)
							foundYear = True
							idxYear = idx

						elif word.lower() in carBrands:
							make = word.lower()	
							# convert from alias to actual car make
							if make in aliasDict:
								make = aliasDict[make]
							foundMake = True	
							idxMake = idx	

					if not foundMake:
						if len(	
		
				else:
					continue



				# attempt to parse year
				if "model_raw" in vehicleDict:
					# model actually contains 3 variables that we'd like: year, manufacturer, and model
					try:
						year = int(vehicleDict["model_raw"][:4])
						if year > nextYear:
							year = None
					except:
						year = None

					model = vehicleDict["model_raw"][5:]
					foundManufacturer = False
					#we parse through each word in the description and search for a match with carBrands (at the top of the program)
					#if a match is found then we have our make, otherwise we set model to the entire string and leave manu blank
					for word in model.split():
						if word.lower() in carBrands:
							foundManufacturer = True
							model = ""
							#resolve conflicting make titles
							make = word.lower()
							if make == "chev" or make == "chevy":
								make = "chevrolet"
							if make == "mercedes" or make == "mercedesbenz":
								make = "mercedes-benz"
							if make == "vw":
								make = "volkswagen"
							if make == "landrover":
								make = "land rover"
							if make == "harley":
								make = "harley-davidson"
							if make == "infinity":
								make = "infiniti"
							if make == "alfa":
								make = "alfa-romeo"
							if make == "aston":
								make = "aston-martin"
							continue
						if foundManufacturer:
							model = model + word.lower() + " "
					model = model.strip()
				
			   
				# fetch the image url if exists
				try:
					img = tree.xpath('//div[@class="slide first visible"]//img')
					image_url = img[0].attrib["src"]
				except:
					pass
				
				# fetch lat/long and city/state if exists
				try:
					location = tree.xpath("//div[@id='map']")
					lat = float(location[0].attrib["data-latitude"])
					long = float(location[0].attrib["data-longitude"])
					
				except:
					pass
				
				# try to fetch a vehicle description
				try:
					location = tree.xpath("//section[@id='postingbody']")
					description = location[0].text_content().strip()
					description = description.rstrip()
					description = description.lstrip()
					description = description.strip("QR Code Link to This Post")
					description = description.lstrip()
				except:
					pass
					
				# produce message to kafka
				msg = json.dumps(vehicleDict)
				producer.produce(msg.encode('utf-8'))			   
  
				#finally we get to insert the entry into the database
				scraped += 1
				 
			#these lines will execute every time we grab a new page (after 120 entries)
			print("{} vehicles scraped".format(scraped))
		  
def main():
	runScraper()


if __name__ == "__main__":
	main()
