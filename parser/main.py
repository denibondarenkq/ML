import requests
from bs4 import BeautifulSoup
import json
import csv
import re
import time
from multiprocessing import Pool


def get_html_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        session = requests.Session()
        response = session.get(url, headers=headers)
        if response.status_code == 403:
            print(f"Access denied for {url}, status code 403. Sleeping for 30 seconds.")
            time.sleep(30)
            return None
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def extract_json_data(soup):
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if script_tag:
        json_data = json.loads(script_tag.string)
        return json_data
    return None


def parse_property_details(json_data, link):
    gdp_cache = json_data['props']['pageProps']['componentProps'].get('gdpClientCache', None)

    if gdp_cache:
        gdp_client_cache_json = json.loads(gdp_cache)
        property_key = list(gdp_client_cache_json.keys())[0]
        property_data = gdp_client_cache_json[property_key]['property']

        price = property_data.get('price', 'N/A')
        bedrooms = property_data.get('bedrooms', 'N/A')
        bathrooms = property_data.get('bathrooms', 'N/A')
        latitude = property_data.get('latitude', 'N/A')
        longitude = property_data.get('longitude', 'N/A')

        neighborhood = property_data.get('parentRegion', {}).get('name', 'N/A')
        built_year = property_data.get('yearBuilt', 'N/A')

        at_a_glance_facts = property_data.get('resoFacts', {}).get('atAGlanceFacts', [])
        if isinstance(at_a_glance_facts, list):
            home_type = next((fact['factValue'] for fact in at_a_glance_facts if fact['factLabel'] == 'Type'), 'N/A')
            home_type = home_type.split(',')[0] if ',' in home_type else home_type
        else:
            home_type = 'N/A'

        return {
            'price': price,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'latitude': latitude,
            'longitude': longitude,
            'neighborhood': neighborhood,
            'built_year': built_year,
            'home_type': home_type,
            'link': link
        }

    return None


def parse_property_card(card):
    link = card.find('a', {'data-test': 'property-card-link'})['href']
    if not link.startswith('http'):
        link = 'https://www.zillow.com' + link
    return link


def get_next_page_url(soup):
    next_page = soup.find('a', {'title': 'Next page'})
    if next_page and 'href' in next_page.attrs:
        return 'https://www.zillow.com' + next_page['href']
    return None


def write_to_csv(data, file_name='real_estate_data.csv'):
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['Neighborhood', 'Type', 'Price', 'Beds', 'Baths', 'Built Year', 'Longitude', 'Latitude', 'Link'])
        writer.writerows(data)


def process_property(link):
    try:
        property_html = get_html_content(link)
        if property_html is None:
            return None
        property_soup = BeautifulSoup(property_html, 'html.parser')
        json_data = extract_json_data(property_soup)

        if json_data:
            property_details = parse_property_details(json_data, link)
            if property_details:
                print(f"Successfully processed property: {link}")
                return [
                    property_details['neighborhood'],
                    property_details['home_type'],
                    property_details['price'],
                    property_details['bedrooms'],
                    property_details['bathrooms'],
                    property_details['built_year'],
                    property_details['longitude'],
                    property_details['latitude'],
                    property_details['link']
                ]
    except Exception as e:
        print(f"Error processing property {link}: {e}")
        return None


def main():
    url = 'https://www.zillow.com/san-francisco-ca/'
    house_count = 0
    target_house_count = 1600
    all_links = []

    while url and house_count < target_house_count:
        html_content = get_html_content(url)
        if html_content is None:
            break

        soup = BeautifulSoup(html_content, 'html.parser')
        property_cards = soup.find_all('article', {'data-test': 'property-card'})
        links = [parse_property_card(card) for card in property_cards]
        all_links.extend(links)

        house_count += len(links)
        print(f"Parsed {house_count} properties so far.")

        if len(all_links) >= target_house_count:
            all_links = all_links[:target_house_count]
            break

        url = get_next_page_url(soup)

    with Pool(processes=8) as pool:
        results = pool.map(process_property, all_links)

    property_data = [result for result in results if result][:target_house_count]
    write_to_csv(property_data)
    print(f"Парсинг завершен, данные сохранены в 'real_estate_data.csv' ({len(property_data)} properties)")

if __name__ == '__main__':
    main()