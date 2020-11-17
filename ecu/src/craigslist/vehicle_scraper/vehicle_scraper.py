#!/usr/bin/env python

from collections import OrderedDict
from datetime import datetime
from lxml import html
from pseudodb import car_brands, brand_aliases
from requests_html import HTMLSession

import argparse
import boto3
import json
import kafkaconnect
import logging
import os
from queue import Queue
import threading

# multi-threading init
city_queue = Queue()
threads = []

# TODO: add logging

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
    # TODO: ok this is really crappy code, needs to be rewritten and logged for analysis/error rate
    foundMake = False
    makeDouble = False
    parsed_title_tokens = raw_model.strip().split(" ")
    ret = {}

    def check_brand_alias(vehicle_make):
        return brand_aliases[vehicle_make] if vehicle_make in brand_aliases else vehicle_make

    # skip otherwise TODO: send to kafka for analysis
    if len(parsed_title_tokens[0]) != 4 and not parsed_title_tokens[0].isnumeric():
        print(parsed_title_tokens)
        return None

    # assuming year is the first word (99%+ case)
    year = parsed_title_tokens[0]
    ret['year'] = int(year)
    parsed_title_tokens.remove(year)

    # if remaining word is a make TODO: shouldn't this just be ignored?
    if len(parsed_title_tokens) == 1 and parsed_title_tokens[0].lower() in car_brands:
        ret['make'] = check_brand_alias(parsed_title_tokens[0].lower())
        return ret

    for idx, word in enumerate(parsed_title_tokens):
        if word.lower() in car_brands:
            make = word.lower()
            if idx + 1 < len(parsed_title_tokens):
                if make + parsed_title_tokens[idx + 1].lower() in car_brands:
                    makeDouble = True
            ret['make'] = check_brand_alias(make)
            foundMake = True
            break
        elif idx + 1 < len(parsed_title_tokens) and \
                word.lower() + parsed_title_tokens[idx + 1].lower() in car_brands:
            ret['make'] = check_brand_alias(word.lower() + parsed_title_tokens[idx + 1].lower())
            foundMake = True
            makeDouble = True
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


def scrape_vehicles_from_city(city, thread_dict):
    vehicles_scraped = 0
    vehicles_scraped_per_city = 0
    empty = False

    # this loop executes until we are out of search results, craigslist sets this limit at 3000
    # and cities often contain the full 3000 records (but not always)
    while not empty:
        print(f"""Gathering entries {vehicles_scraped_per_city} through {vehicles_scraped_per_city
                                                                         + 120} in city {city}""")

        try:
            searchUrl = f"{city['url']}/d/cars-trucks/search/cta?s={vehicles_scraped_per_city}"
            page = thread_dict.session.get(searchUrl)
        except Exception as e:
            # catch any excpetion and continue the loop if we cannot access a site for whatever
            # reason
            print(f"Failed to reach {searchUrl}, entries have been dropped: {e}")
            continue

        # Each search page contains 120 entries
        vehicles_scraped_per_city += 120
        tree = html.fromstring(page.content)

        # the following line returns a list of urls for different vehicles
        # vehicles = tree.xpath('//a[@class="result-image gallery"]')
        vehicles = tree.xpath('//p[@class="result-info"]')

        if len(vehicles) == 0:
            # if we no longer have entries, continue to the next city
            empty = True
            continue

        vehicles_list = []
        for item in vehicles:
            vehicle_details = []

            dt = item[1].attrib['datetime']
            # filter date by today's date WARNING (run only from 9AM-5PM UTC))
            # TEST MODE ON
            # if dt.split(' ')[0] != datetime.utcnow().strftime('%Y-%m-%d'):
            #	continue

            vehicle_details.append(item[2].attrib["href"])

            try:
                # attempt to grab the price of the vehicle. some vehicles dont have prices (which
                # throws an exception) and we dont want those which is why we toss them
                vehicle_details.append(item[3][0].text)
            except Exception as e:
                print(e)
                continue

            vehicle_details.append(dt)
            vehicles_list.append(vehicle_details)

        # loop through each vehicle
        for item in vehicles_list:
            url = item[0]
            vehicle_dict = {
                "price": int(item[1].strip("$")),
                "city": city['name'],
                "datetime": item[2]
            }

            try:
                # grab each individual vehicle page
                page = thread_dict.session.get(url)
                tree = html.fromstring(page.content)
            except Exception as e:
                print(f"{e}: Failed to reach {url}, entry has been dropped")
                continue

            attrs = tree.xpath('//span//b')
            # this fetches a list of attributes about a given vehicle. each vehicle does not have
            # every specific attribute listed on craigslist so this code gets a little messy as
            # we need to handle errors if a car does not have the attribute we're looking for
            for att in attrs:
                try:
                    # model is the only attribute without a specific tag on craigslist
                    # if this code fails it means that we've grabbed the model of the vehicle
                    k = att.getparent().text.strip()
                    k = k.strip(":")
                except Exception as e:
                    print(e)
                    k = "model_raw"
                try:
                    # this code fails if item=None so we have to handle it appropriately
                    vehicle_dict[k] = att.text.strip()
                except Exception as e:
                    print(e)
                    continue

            # TODO: find a standardized way to store make and model (hacky solution rn)
            if "model_raw" in vehicle_dict:
                parsed = parse_model(vehicle_dict['model_raw'])
                if parsed is None:
                    continue
                else:
                    vehicle_dict.update(parsed)
            else:
                continue

            # fetch the image url if exists
            try:
                img = tree.xpath('//div[@class="slide first visible"]//img')
                vehicle_dict['image_url'] = img[0].attrib["src"]
            except Exception as e:
                print(e)
                pass

            # fetch lat/long and city/state if exists
            try:
                location = tree.xpath("//div[@id='map']")
                vehicle_dict['lat'] = float(location[0].attrib["data-latitude"])
                vehicle_dict['lon'] = float(location[0].attrib["data-longitude"])
            except Exception as e:
                print(e)
                pass

            # try to fetch a vehicle description
            try:
                location = tree.xpath("//section[@id='postingbody']")
                description = location[0].text_content().strip()
                description = description.strip()
                description = description.strip("QR Code Link to This Post")
                vehicle_dict['description'] = description.lstrip()
            except Exception as e:
                print(e)
                pass

            # produce message to kafka
            msg = json.dumps(vehicle_dict)
            # print("vehicle_dict:", vehicle_dict)
            thread_dict.producer.produce(msg.encode('utf-8'))
            # finally we get to insert the entry into the database
            vehicles_scraped += 1

        # these lines will execute every time we grab a new page (after 120 entries)
        print("{} vehicles vehicles_scraped".format(vehicles_scraped))


def threader(thread_dict):
    client, producer = kafkaconnect.connect(hosts, topics)
    thread_dict.producer = producer
    thread_dict.session = HTMLSession()

    while True:
        item = city_queue.get()
        if item is None:
            break
        scrape_vehicles_from_city(item, thread_dict)
        city_queue.task_done()

    thread_dict.producer.stop()


def init_threading():
    threadDict = threading.local()
    threadDict.producer = None
    threadDict.session = None

    for i in range(threadCount):
        t = threading.Thread(target=threader, args=(threadDict,))
        t.daemon = True
        t.start()
        threads.append(t)


def populate_queue():
    with open('cities.txt') as json_file:
        cities = json.load(json_file)
        for key, value in cities.items():
            city_queue.put(value)


def stop_threads():
    city_queue.join()
    # stop workers
    for i in range(threadCount):
        city_queue.put(None)
    for t in threads:
        t.join()


def main():
    s3_init()
    populate_queue()
    init_threading()
    stop_threads()


if __name__ == "__main__":
    main()
