#!/usr/bin/env python

from collections import OrderedDict
from datetime import datetime
from lxml import html
from requests_html import HTMLSession

import argparse
import boto3
import json
import kafkaconnect
import logging
import os
from pseudodb import *
from queue import Queue
import threading

# multi-threading init
city_queue = Queue()
threads = []


# parse arguments TODO: make these env vars
parser = argparse.ArgumentParser()
parser.add_argument("--hosts", required=True, default="localhost:9092")
parser.add_argument("--topics", required=True, default="vehicle_scraper")
parser.add_argument("--threads", required=True, default="4")

argument = parser.parse_args()
hosts = str(argument.hosts)
topics = str(argument.topics)
threadCount = int(argument.threads)


def s3_init():
    # connect to s3, download cities file TODO: ENV aws credentials / iam role
    s3_session = boto3.Session(profile_name=os.environ.get('AWS_PROFILE_NAME', 'omkar'))
    s3 = s3_session.resource('s3')
    bucket = s3.Bucket('citiesscraped')
    bucket.download_file('cities.txt', 'cities.txt')


def parse_model(raw_model):
    found_year = False
    foundMake = False
    makeDouble = False
    idxMake = None
    parsed_title_tokens = raw_model.strip().split(" ")
    make = None
    ret = {}

    def check_brand_alias(make):
        return aliasDict[make] if make in aliasDict else make

    # skip otherwise
    if len(parsed_title_tokens[0]) == 4 and parsed_title_tokens[0].isnumeric():
        found_year = True
    else:
        print(parsed_title_tokens)
        return None

    # assuming year is the first word (99%+ case)
    year = int(parsed_title_tokens[0])
    ret['year'] = year
    parsed_title_tokens.remove(parsed_title_tokens[0])

    # if remaining word is a make
    if len(parsed_title_tokens) == 1 and parsed_title_tokens[0].lower() in carBrands:
        ret['make'] = check_brand_alias(parsed_title_tokens[0].lower())
        return ret

    for idx, word in enumerate(parsed_title_tokens):
        if word.lower() in carBrands:
            make = word.lower()
            if idx + 1 < len(parsed_title_tokens):
                if make + parsed_title_tokens[idx + 1].lower() in carBrands:
                    idxMake = idx
                    makeDouble = True
            ret['make'] = check_brand_alias(make)
            foundMake = True
            idxMake = idx
            break
        elif idx + 1 < len(parsed_title_tokens) and word.lower() + parsed_title_tokens[idx + 1].lower() in carBrands:
            ret['make'] = check_brand_alias(word.lower() + parsed_title_tokens[idx + 1].lower())
            foundMake = True
            makeDouble = True
            idxMake = idx
            break
    if makeDouble:
        del parsed_title_tokens[idx]
        del parsed_title_tokens[idx]
    elif foundMake:
        del parsed_title_tokens[idx]
    else:
        return None

    if parsed_title_tokens:
        ret['model'] = " ".join(parsed_title_tokens)

    return ret


def cityScrape(city, threadDict):
    scraped = 0
    scrapedInCity = 0
    empty = False

    # this loop executes until we are out of search results, craigslist sets this limit at 3000
    # and cities often contain the full 3000 records (but not always)
    while not empty:
        print(f"Gathering entries {scrapedInCity} through {scrapedInCity + 120}")

        # now we scrape
        try:
            searchUrl = f"{city['url']}/d/cars-trucks/search/cta?s={scrapedInCity}"
            page = threadDict.session.get(searchUrl)
        except Exception as e:
            # catch any excpetion and continue the loop if we cannot access a site for whatever reason
            print(f"Failed to reach {searchUrl}, entries have been dropped: {e}")
            continue

        # each search page contains 120 entries
        scrapedInCity += 120
        tree = html.fromstring(page.content)

        # the following line returns a list of urls for different vehicles
        # vehicles = tree.xpath('//a[@class="result-image gallery"]')
        vehicles = tree.xpath('//p[@class="result-info"]')

        if len(vehicles) == 0:
            # if we no longer have entries, continue to the next city
            empty = True
            continue

        vehiclesList = []
        for item in vehicles:
            vehicleDetails = []

            dt = item[1].attrib['datetime']
            # filter date by today's date WARNING (run only from 9AM-5PM UTC))
            """ TEST MODE ON
			if dt.split(' ')[0] != datetime.utcnow().strftime('%Y-%m-%d'):
				continue
			"""

            vehicleDetails.append(item[2].attrib["href"])
            try:
                # attempt to grab the price of the vehicle. some vehicles dont have prices (which throws an exception)
                # and we dont want those which is why we toss them
                vehicleDetails.append(item[3][0].text)
            except:
                continue

            vehicleDetails.append(dt)
            vehiclesList.append(vehicleDetails)

        # loop through each vehicle
        for item in vehiclesList:
            url = item[0]

            vehicleDict = {}
            vehicleDict["price"] = int(item[1].strip("$"))
            vehicleDict["city"] = city['name']
            vehicleDict["datetime"] = item[2]
            try:
                # grab each individual vehicle page
                page = threadDict.session.get(url)
                tree = html.fromstring(page.content)
            except:
                print(f"Failed to reach {url}, entry has been dropped")
                continue

            attrs = tree.xpath('//span//b')
            # this fetches a list of attributes about a given vehicle. each vehicle does not have every specific attribute listed on craigslist
            # so this code gets a little messy as we need to handle errors if a car does not have the attribute we're looking for
            for att in attrs:
                try:
                    # model is the only attribute without a specific tag on craigslist
                    # if this code fails it means that we've grabbed the model of the vehicle
                    k = att.getparent().text.strip()
                    k = k.strip(":")
                except:
                    k = "model_raw"
                try:
                    # this code fails if item=None so we have to handle it appropriately
                    vehicleDict[k] = att.text.strip()
                except:
                    continue

            # TODO: find a standardized way to store make and model (hacky solution rn)
            if "model_raw" in vehicleDict:
                parsed = parse_model(vehicleDict['model_raw'])
                if parsed is None:
                    continue
                else:
                    vehicleDict.update(parsed)
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
            # print("vehicleDict:", vehicleDict)
            threadDict.producer.produce(msg.encode('utf-8'))
            # finally we get to insert the entry into the database
            scraped += 1

        # these lines will execute every time we grab a new page (after 120 entries)
        print("{} vehicles scraped".format(scraped))


def threader(threadDict):
    client, producer = kafkaconnect.connect(hosts, topics)
    threadDict.producer = producer
    threadDict.session = HTMLSession()

    while True:
        item = city_queue.get()
        if item is None:
            break
        cityScrape(item, threadDict)
        city_queue.task_done()

    threadDict.producer.stop()


def initThreading():
    threadDict = threading.local()
    threadDict.producer = None
    threadDict.session = None

    for i in range(threadCount):
        t = threading.Thread(target=threader, args=(threadDict,))
        t.daemon = True
        t.start()
        threads.append(t)


def populateQueue():
    with open('cities.txt') as json_file:
        cities = json.load(json_file)
        for key, value in cities.items():
            city_queue.put(value)


def stopThreads():
    city_queue.join()
    # stop workers
    for i in range(threadCount):
        city_queue.put(None)
    for t in threads:
        t.join()


def main():
    s3_init()
    populateQueue()
    initThreading()
    stopThreads()


if __name__ == "__main__":
    main()
