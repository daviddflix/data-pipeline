import json
from urllib.parse import quote
from lxml import etree
import requests
import time
from bs4 import BeautifulSoup
import os

# Define the maximum time in seconds to extract the content of a news article
MAX_EXTRACTION_TIME = 10  # 10 seconds as a time limit example

def find_scraper_result_files(directory_path):
    """Finds all files in the specified directory that contain 'Scrape_Result' in their name."""
    scraper_files = []
    all_files = []

    # Traverse the directory and gather all files
    for root, _, files in os.walk(directory_path):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)  # Add all files to the list for display purposes
            if "Scrape_result" in file:
                scraper_files.append(full_path)  # Add only Scrape_Result files to process

    # Print all files in the directory
    print(f"All files in the directory '{directory_path}':")
    for file in all_files:
        print(f" - {file}")

    return scraper_files

def get_google_params(url):
    """Extract the required parameters to build the real URL from Google News."""
    response = requests.get(url)
    tree = etree.HTML(response.text)

    try:
        sign = tree.xpath('//c-wiz/div/@data-n-a-sg')[0]
        ts = tree.xpath('//c-wiz/div/@data-n-a-ts')[0]
        source = tree.xpath('//c-wiz/div/@data-n-a-id')[0]
        return source, sign, ts
    except IndexError as e:
        print(f"Error extracting parameters: {e}")
        return None, None, None

def get_origin_url(source, sign, ts):
    """Builds the real URL from the extracted parameters."""
    if not all([source, sign, ts]):
        print("Missing required parameters to generate the request.")
        return None

    url = f"https://news.google.com/_/DotsSplashUi/data/batchexecute"
    req_data = [[[ 
        "Fbv4je",
        f"[\"garturlreq\",[[\"zh-HK\",\"HK\",[\"FINANCE_TOP_INDICES\",\"WEB_TEST_1_0_0\"],null,null,1,1,\"HK:zh-Hant\",null,480,null,null,null,null,null,0,5],\"zh-HK\",\"HK\",1,[2,4,8],1,1,null,0,0,null,0],\"{source}\",{ts},\"{sign}\"]",
        None,
        "generic"
    ]]]
    payload = f"f.req={quote(json.dumps(req_data))}"
    headers = {
        'Host': 'news.google.com',
        'X-Same-Domain': '1',
        'Accept-Language': 'zh-CN',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'Accept': '*/*',
        'Origin': 'https://news.google.com',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://news.google.com/',
        'Accept-Encoding': 'gzip, deflate, br',
    }
    
    # Execute the POST request
    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        return None

    response_text = response.text[5:]  # Remove the prefix ")]}'"

    try:
        json_response = json.loads(response_text)
        for item in json_response[0]:
            if isinstance(item, str) and "garturlres" in item:
                url_data = json.loads(item)
                extracted_url = url_data[1]
                return extracted_url
    except (json.JSONDecodeError, IndexError) as e:
        print(f"Error processing JSON response: {e}")
        return None

def get_real_url(url):
    """Gets the real URL of the news article from the Google News URL."""
    source, sign, ts = get_google_params(url)
    origin_url = get_origin_url(source, sign, ts)
    print(f"Extracted URL: {origin_url}")
    return origin_url

def load_links_from_txt(txt_path):
    """Loads links from a text file and separates them by commas from continuous lines."""
    links = []
    capture_links = False

    with open(txt_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "Links:" in line:
                capture_links = True
                continue

            if capture_links:
                if line.strip() == "":
                    break
                extracted_links = [link.strip() for link in line.split(',') if link.strip()]
                links.extend(extracted_links)

    return links

def extract_news_content(url):
    """Extracts the main content of a news article given its URL within a time limit."""
    try:
        response = requests.get(url, timeout=MAX_EXTRACTION_TIME)  # Add time limit to the request
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('title').get_text() if soup.find('title') else 'No Title'
            date = soup.find('time').get_text() if soup.find('time') else 'Date not available'
            paragraphs = soup.find_all('p')
            content = " ".join([p.get_text() for p in paragraphs])

            if not content:
                content = soup.get_text()

            return title, date, content
        else:
            print(f"Error accessing URL: {url}")
            return None, None, None

    except requests.Timeout:
        print(f"Extraction time exceeded for URL: {url}")
        return None, None, None

    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None, None, None

def save_to_consolidated_file(title, date, content, output_path):
    """Saves the news article in a consolidated file with all extracted news."""
    with open(output_path, 'a', encoding='utf-8') as file:
        file.write(f"Title: {title}\n")
        file.write(f"Date: {date}\n")
        file.write("Content:\n")
        file.write(content)
        file.write("\n" + "="*80 + "\n")
    print(f"News saved to consolidated file: {output_path}")


def process_news_from_directory(input_directory, consolidated_file_path):
    """Processes all 'Scrape_Result' files in the input directory and consolidates the news into the specified file."""
    scraper_files = find_scraper_result_files(input_directory)

    # Print the specific files to be processed
    print(f"\nFound {len(scraper_files)} file(s) containing 'Scrape_Result' to process:")
    for file in scraper_files:
        print(f" - {file}")

    # Create a new consolidated file
    with open(consolidated_file_path, 'w', encoding='utf-8') as f:
        f.write("Consolidated News\n")
        f.write("="*80 + "\n\n")

    # Process each file in the directory
    for scraper_file in scraper_files:
        print(f"\n--- Processing file: {scraper_file} ---\n")
        links = load_links_from_txt(scraper_file)

        for link in links:
            real_url = get_real_url(link)
            if real_url:
                print(f"Real URL: {real_url}")
                title, date, content = extract_news_content(real_url)
                if content:
                    print(f"Content extracted from the news article ({title[:30]}...)\n")
                    save_to_consolidated_file(title, date, content, consolidated_file_path)
                else:
                    print(f"Could not extract content for the news article at {real_url}\n")
            else:
                print(f"Could not retrieve the real URL for {link}\n")

    print(f"Consolidated file saved at: {consolidated_file_path}")

# Example usage:
#process_news_from_directory("/home/lourdes22/data-pipeline/files", "/home/lourdes22/data-pipeline/files/consolidated_news_output.txt", "/home/lourdes22/data-pipeline/files/consolidated_news_output.txt")
