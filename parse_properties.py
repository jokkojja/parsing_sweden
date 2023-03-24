from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pymongo import InsertOne
import os
import re
import time as tm
import asyncio
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient
import aiohttp
import ast
import numpy as np
import random
from config import SCRAPPER_API_KEY, possible_keys, my_col, proxy
import json

res = []

with open('property_urls.txt', 'r+') as file:
    urls = np.unique(np.array(file.read().splitlines()))[:20]
@dataclass
class PropertyItem:
    """ Class for keeping attributes of property
    """
    accommodationCost: str = field(default=None)
    squareMeterPrice: int = field(default=None)
    yearOfConstruction: int = field(default=None)
    releaseForm: str = field(default=None)
    housingType: str = field(default=None)
    daysAtBooli: int = field(default=None)
    pageViews: int = field(default=None)
    plotSize: int = field(default=None)
    operatingCost: int = field(default=None)
    url: str = field(default=None)
    priceSEK: float = field(default=None)
    publishDate: datetime = field(default=None)
    rooms: int = field(default=None)
    floor: int = field(default=None)
    sqrArea: float = field(default=None)
    postalAddress: dict = field(default=None)
    listingId: int = field(default=None)
    coordinates: dict = field(default=None)
    charge: int = field(default=None)
    beeAreaSquareMeter: int = field(default=None)
        
async def gather_active_urls(proxy=proxy):
    tasks = []
    semaphore = asyncio.Semaphore(20)
        
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=3)) as session:
        for url in urls:
            task = asyncio.create_task(parse_properties(session, url, semaphore))
            tasks.append(task)
            print(f'Added {url}')
        await asyncio.gather(*tasks) 
        
async def parse_properties(session, property_url: str, semaphore, proxy:str = proxy) -> None:
    item = PropertyItem()
    await semaphore.acquire()
    try:
        async with session.get(property_url, proxy = proxy) as response:
            number_pattern = '\d+'
            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')
            property_table =  soup.find_all('div', class_ = 'DfWRI _1Pdm1 _2zXIc sVQc-') # get table with all data about propertyy
            scripts = str(soup.find_all('script', string=True)) # get all scripts
            lon_pat = '"longitude":\d+.\d+' # regex pattern for longitude
            lat_pat = '"latitude":\d+.\d+' # regex pattern for latitude
            latitude = ast.literal_eval('{' + re.findall(lat_pat, scripts)[0].replace(':', ' : ') + '}') # extract latitude from script
            longitude = ast.literal_eval('{' + re.findall(lon_pat, scripts)[0].replace(':', ' : ') + '}') # extract longitude from script

            item.coordinates = {**latitude, **longitude} # unpack coordinates

            try:
                price = int(re.findall('\d{1,3}(?:\s\d{3})*(?:[,.]\d+)? kr', 
                                    str(soup.findAll('h2', class_ = 'lzFZY _10w08')))[0][:-3].replace(' ', ''))
                item.priceSEK = price
            except (AttributeError, ValueError, IndexError):
                print(property_url)
                pass

            table = {}
            for elem in property_table:
                try:
                    part_of_table = elem.find('div', class_ = '_18w8g').text
                    if part_of_table.isdigit():
                        part_of_table = int(part_of_table)
                    item.__setattr__(possible_keys[elem.find('div', class_ = '_2soQI').text], part_of_table)
                except AttributeError:
                    pass

            postal_address = soup.find_all('script', type = 'application/ld+json', string=True)
            postal_address = json.loads(postal_address[2].string)['address']
            postal_address['postalCode'] = int(postal_address['postalCode'])
            postal_address.pop('@type')
            item.postalAddress = postal_address
            try:
                item.charge = int(''.join(re.findall(number_pattern, item.charge)))
            except (TypeError, ValueError):
                pass 

            try:
                item.beeAreaSquareMeter = int(''.join(re.findall(number_pattern, item.beeAreaSquareMeter)))
            except (TypeError, ValueError):
                pass 

            try:
                item.operatingCost = int(''.join(re.findall(number_pattern, item.operatingCost)))
            except (TypeError, ValueError):
                pass

            try:
                item.accommodationCost = int(''.join(re.findall(number_pattern, item.accommodationCost)))
            except (TypeError, ValueError):
                pass   

            try:
                item.squareMeterPrice = int(''.join(re.findall(number_pattern, item.squareMeterPrice)))
            except (TypeError, ValueError):
                pass

            try:
                item.plotSize = int(''.join(re.findall(number_pattern, item.plotSize)))
            except (TypeError, ValueError):
                pass    

            item.url = property_url
            
            try:
                rooms = int(re.findall('\d+', re.findall('\d+ rum',str(soup.findAll('h4', class_ = '_1544W _10w08')))[0])[0])
                item.rooms = rooms
            except IndexError:
                pass

            try:
                area = re.findall('\d+', re.findall('(\d \d+ m²|\d+ m²)', str(soup.findAll('h4', class_ = '_1544W _10w08')))[0])
                area = int(''.join(area))                
                item.sqrMetersArea = area
            except IndexError:
                pass

            try:
                item.floor = int(re.findall(number_pattern, item.floor)[0])
            except (TypeError, ValueError):
                pass

            try:
                date = datetime.today()
                result_date = date - timedelta(days=item.daysAtBooli)
                item.publishDate = result_date
            except (KeyError, TypeError):
                pass

            item.listingId = int(property_url.split('/')[-1])

            table = asdict(item)
            print(f'Processed {property_url}')

            my_col.bulk_write([InsertOne(table)])
    except asyncio.TimeoutError:
        print('Something wrong')    
        
if __name__ == '__main__':
    asyncio.run(gather_active_urls())