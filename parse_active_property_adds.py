import asyncio
import os
import re
import time as tm
from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient
import aiohttp
from config import SCRAPPER_API_KEY, proxy

start_time = tm.time()
urls = []
failed_urls = []
    
def parse_booli_url_with_propeties() -> None:
    """ Preparing txt with
    """
    try:
        os.remove('urls.txt')
    except OSError:
        pass
    client = ScraperAPIClient(SCRAPPER_API_KEY)
    url = 'https://www.booli.se/alla_bostader/'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    resp = client.get(url=url, headers=headers)
    soup = BeautifulSoup(resp.text, 'lxml')
    list_of_level_1_links = soup.findAll('h3')
    url_with_property_urls = []
    for i, level in enumerate(list_of_level_1_links): # creating first level of locations
        location_url = 'https://www.booli.se' + level.find('a').get('href')
        resp = client.get(url=location_url, headers=headers)
        soup = BeautifulSoup(resp.text, 'lxml')
        active_adds_url = 'https://www.booli.se' + soup.find('div', class_="content").findAll('h2')[0].find('a').get('href')
        url_with_property_urls.append(active_adds_url)
        with open('urls.txt', 'a+') as file:
            file.write(active_adds_url + '\n')
        print(f'progress {i+1}/{len(list_of_level_1_links)}', end = '\r')
    return url_with_property_urls
            
async def gather_properties_urls(retry_failed: bool = False, proxy = proxy):
    """ Creating tasks for extracting properies urls
    """
    tasks = []
    semaphore = asyncio.Semaphore(20)
    if retry_failed:
        print('\nStarted extracting property urls from failed urls\n')
        with open('failed_urls.txt', 'r') as file:
            failed_urls = file.read().splitlines()
        os.remove('failed_urls.txt')
        
        if len(failed_urls) == 0:
            print("Failed urls don't exists")
            return
        
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=3)) as session:
            for url in failed_urls:
                task = asyncio.create_task(parse_properties_urls(session, url, semaphore))
                tasks.append(task)
            await asyncio.gather(*tasks)
                
        
        
    else:  
        print('Started extracting property urls from all urls')    
        with open('urls.txt', 'r') as file:
            urls = file.read().splitlines()
        
        try:
            os.remove('property_urls.txt')
        except OSError:
            pass
                 
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, limit=3)) as session:
            for i, url in enumerate(urls):
                try:
                    response = await session.get(url=url, proxy = proxy)
                    soup = BeautifulSoup(await response.text(), "lxml")
                    
                except asyncio.TimeoutError as e:
                    with open('timeout_error.txt', 'a+') as file:
                        file.write(url + '\n')
                    continue
                
                try:
                    count_str = soup.find('h1', class_='_1GhAx _2g-o0').text
                    str_with_count_of_properties = re.search('Hitta bostad(.*)bostäder till salu', count_str).group(1)
                    count_of_properties = re.findall(r'\d+', str_with_count_of_properties)
                    count_of_pages = int(''.join(count_of_properties))//36 + 1
                except AttributeError: #failed request
                    with open('timeout_error.txt', 'a+') as file:
                        file.write(url + '\n')
                    continue                    
                    
                if count_of_pages > 1000:
                    count_of_pages = 1000
                    
                for page in range(1, count_of_pages + 1):
                    pagination_url =  url + f"?page={page}"
                    task = asyncio.create_task(parse_properties_urls(session, pagination_url, semaphore))
                    tasks.append(task)
                print(f"progress {i+1}/{len(urls)}", end = '\r')
                    
            await asyncio.gather(*tasks)
            
async def parse_properties_urls(session, pagination_url, semaphore, proxy = proxy):
    await semaphore.acquire()
    try:
        async with session.get(pagination_url, proxy = proxy) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, "lxml")
            try:
                div_with_urls = soup.find('div', class_ = '_2m6km _3IyMn _3VU6q _1EK1o').findAll('a')
                for tag in div_with_urls:
                    url = 'https://www.booli.se' + tag.get('href')
                    if 'projekt' not in url:
                        urls.append(url)  
                                    
            except AttributeError: # request failed
                failed_urls.append(pagination_url)
    except asyncio.TimeoutError:
        with open('timeout_error.txt', 'a+') as file:
            file.write(pagination_url + '\n')
    print(f"Processed {pagination_url}")
    semaphore.release()
    
def write_urls_to_file(filename: str, urls: list):
    with open(filename, 'a+') as file:
        for url in urls:
            file.write(url + '\n') 
               
if __name__ == '__main__':
    # parse_booli_url_with_propeties()
    asyncio.run(gather_properties_urls()) # asynco get property urls
    write_urls_to_file('property_urls.txt', urls)
    urls = []
    write_urls_to_file('failed_urls.txt', failed_urls)
    asyncio.run(gather_properties_urls(retry_failed=True)) # get urls whick failed previosly
    write_urls_to_file('property_urls.txt', urls) # add new property urls
    finish_time = tm.time() - start_time
    print(f"Elapsed time: {finish_time}")     

    