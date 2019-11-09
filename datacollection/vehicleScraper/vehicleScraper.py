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
from pseudodb import *
from queue import Queue
from requests_html import HTMLSession
import threading

# multithreading init
cityQueue = Queue()
threads = []

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

def parse_model(raw_model):
        foundYear = False
        foundMake = False
        makeDouble = False
        idxMake = None
        modelWords = raw_model.strip().split(" ")
        year = None
        make = None
        ret = {}

        def checkAlias(makeAlias):
                return aliasDict[makeAlias] if makeAlias in aliasDict else makeAlias

        # assuming year is the first word (99%+ case)
        if len(modelWords[0]) == 4 and modelWords[0].isnumeric():
                foundYear = True
                year = int(modelWords[0])
                ret['year'] = year
                modelWords.remove(modelWords[0])
        # skip otherwise
        else:
                print(modelWords)
                return None
        # if remaining word is a make
        if len(modelWords) == 1 and modelWords[0].lower() in carBrands:
                ret['make'] = checkAlias(modelWords[0].lower())
                return ret

        for idx, word in enumerate(modelWords):
                if word.lower() in carBrands:
                        make = word.lower()
                        if idx+1 < len(modelWords):
                                if make + modelWords[idx+1].lower() in carBrands:
                                        idxMake = idx
                                        makeDouble = True
                        ret['make'] = checkAlias(make)
                        foundMake = True
                        idxMake = idx
                        break
                elif idx+1 < len(modelWords) and word.lower() + modelWords[idx+1].lower() in carBrands:
                        ret['make'] = checkAlias(word.lower() + modelWords[idx+1].lower())
                        foundMake = True
                        makeDouble = True
                        idxMake = idx
                        break
        if makeDouble:
                del modelWords[idx]
                del modelWords[idx]
        elif foundMake:
                del modelWords[idx]
        else:
                return None

        if modelWords:
                ret['model'] = " ".join(modelWords)

        return  ret



def cityScrape(city, threadDict):
	scraped = 0
	scrapedInCity = 0
	empty = False
	
	#this loop executes until we are out of search results, craigslist sets this limit at 3000 and cities often contain the full 3000 records (but not always)		  
	while not empty:
		print(f"Gathering entries {scrapedInCity} through {scrapedInCity + 120}")
		
		#now we scrape
		try:
			searchUrl = f"{city['url']}/d/cars-trucks/search/cta?s={scrapedInCity}"
			page = threadDict.session.get(searchUrl)
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
			
			vehicleDict = {}
			vehicleDict["price"] = int(item[1].strip("$"))
			vehicleDict["city"] = city['name']
			try:
				#grab each individual vehicle page
				page = threadDict.session.get(url)
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
				
						
			# TODO: find a standardized way to store make and model (hacky solution rn)
			if "model_raw" in vehicleDict:
				parsed = parse_model(vehicleDict['model_raw'])
				if parsed is None: continue
				else: vehicleDict.update(parsed)		
			else:
				continue

					   
			# fetch the image url if exists
			try:
				img = tree.xpath('//div[@class="slide first visible"]//img')
				vehicleDict['image_url'] = img[0].attrib["src"]
			except:
				pass
			
			# fetch lat/long and city/state if exists
			try:
				location = tree.xpath("//div[@id='map']")
				vehicleDict['lat'] = float(location[0].attrib["data-latitude"])
				vehicle['lon'] = float(location[0].attrib["data-longitude"])
				
			except:
				pass
			
			# try to fetch a vehicle description
			try:
				location = tree.xpath("//section[@id='postingbody']")
				description = location[0].text_content().strip()
				description = description.strip()
				description = description.strip("QR Code Link to This Post")
				vehicleDict['description'] = description.lstrip()
			except:
				pass
				
			# produce message to kafka
			msg = json.dumps(vehicleDict)
			threadDict.producer.produce(msg.encode('utf-8'))			   
			#finally we get to insert the entry into the database
			scraped += 1
			 
		#these lines will execute every time we grab a new page (after 120 entries)
		print("{} vehicles scraped".format(scraped))
		 


def threader(threadDict):
	client, producer = kafkaconnect.connect(hosts, topics)
	threadDict.producer = producer
	threadDict.session = HTMLSession()

	while True:
		item = cityQueue.get()
		if item is None:
			break
		cityScrape(item, threadDict)
		cityQueue.task_done()


def initThreading():
	threadDict = threading.local()
	threadDict.producer = None
	threadDict.session = None
	
	for i in range(4):
		t = threading.Thread(target=threader, args=(threadDict,))
		t.daemon = True
		t.start()
		threads.append(t)


def populateQueue():
	with open('cities.txt') as json_file:
		cities = json.load(json_file)
		for key, value in cities.items():
			cityQueue.put(value)

def stopThreads():
	cityQueue.join()
	# stop workers
	for i in range(4):
    		cityQueue.put(None)
	for t in threads:
    		t.join()
		
def main():
	populateQueue()
	print("populated queue")
	initThreading()
	stopThreads()

if __name__ == "__main__":
	main()
