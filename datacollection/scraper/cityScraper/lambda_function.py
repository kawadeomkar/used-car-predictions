#crawlCities grabs every city on Craigslist
import boto3
import json
from lxml import html
from requests_html import HTMLSession


def lambda_handler(event, context):
    #create requests session
    s = HTMLSession()
    
    #webpage 'origin' contains all US craigslist regions
    origin = s.get("https://geo.craigslist.org/iso/us/")
    tree = (html.fromstring(origin.content))
    
    #cities = list of elements for each region
    cities = tree.xpath('//ul[@class="height6 geo-site-list"]//li//a')
    
    city_json = {} 
    counter = 0

    for item in cities:
        name = item.text
       
        #if name == None, text is in bold
        if name == None:
            name = item.xpath("//b")[boldAt].text
        
        city_json[counter] = {}
        city_json[counter]["url"] = item.attrib['href']
        city_json[counter]["name"] = name.replace("'", "''")
        
    # pass to s3
    #s3 = boto3.resource('s3')
    #s3Object = s3.Object('CL_CITIES', 'cities.json')
    #s3Object.put(Body=(bytes(json.dumps(city_json).encode('UTF-8'))))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done crawling for cities')
    } 
