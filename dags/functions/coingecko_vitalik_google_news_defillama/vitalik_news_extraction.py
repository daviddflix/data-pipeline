import requests
from bs4 import BeautifulSoup
import re
import os

# Function to find the file containing "Scrape_Result" in its name within the specified directory
def find_scraper_result_file(directory_path):
    """Finds the first file in the specified directory that contains 'Scrape_Result' in its name."""
    for root, _, files in os.walk(directory_path):
        for file in files:
            if "Scrape_result" in file:
                return os.path.join(root, file)  # Return the full path of the file
    print(f"No 'Scrape_Result' file found in the directory: {directory_path}")
    return None

# Function to load links from a text file and separate them by commas from continuous lines
def load_links_from_txt(txt_path):
    """Loads links from a text file and separates them by commas from continuous lines."""
    links = []
    capture_links = False  # Flag to indicate when to start capturing links

    # Read the file line by line
    print(f"--- Starting file reading: {txt_path} ---")
    with open(txt_path, 'r', encoding='utf-8') as file:
        for line in file:
            print(f"Read line: {line.strip()}")  # Print the line as read
            if "Links:" in line:
                print("Found 'Links:' marker, starting to capture links...")
                capture_links = True
                continue  # Skip the "Links:" line
            if capture_links:
                if line.strip() == "":
                    print("Empty line found, stopping link capture.")
                    break
                extracted_links = [link.strip() for link in line.split(',') if link.strip()]
                print(f"Links extracted from the line: {extracted_links}")  # Print extracted links
                links.extend(extracted_links)

    links = list(set(links))  # Remove duplicates
    print(f"Links loaded and duplicates removed: {links}")  # Print the complete list of links
    return links  # Return the complete list of links

# Function to get the real URL from Google News
def get_real_url(google_news_url):
    """Gets the real URL of the article from the Google News link."""
    try:
        response = requests.get(google_news_url)
        if response.status_code == 200:
            match = re.search(r'href="(https://[^"]+)"', response.text)
            if match:
                return match.group(1)
        return google_news_url  # If redirection is not found, return the same URL
    except Exception as e:
        print(f"Error getting the real URL: {e}")
        return None

# Function to extract the news content
def extract_news_content(url):
    """Extracts the main content of a news article given its URL and returns the title, date, and body."""
    try:
        response = requests.get(url)
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
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return None, None, None

# Function to save the content in a consolidated file
def save_to_consolidated_file(title, date, content, output_path):
    """Saves the news article in a consolidated file with all extracted news."""
    with open(output_path, 'a', encoding='utf-8') as file:
        file.write(f"Title: {title}\n")
        file.write(f"Date: {date}\n")
        file.write("Content:\n")
        file.write(content)
        file.write("\n" + "="*80 + "\n")
    print(f"News saved: {title}")

# Main function with a more descriptive name
def execute_news_extraction_pipeline(scraper_directory, consolidated_directory):
    """
    Executes the complete news extraction pipeline:
    1. Finds the 'Scrape_Result' file in the specified directory.
    2. Extracts and processes news article URLs.
    3. Retrieves content for each article.
    4. Saves the consolidated news content to a specified output file.
    """
    # Find the first Scrape_Result file in the given directory
    txt_file_path = find_scraper_result_file(scraper_directory)
    if not txt_file_path:
        print("No Scrape_Result file found. Exiting...")
        return

    # Define the path of the consolidated file in the specified directory
    consolidated_file_path = os.path.join(consolidated_directory, "consolidated_vitalik_news.txt")

    # Load the links from the TXT file
    links = load_links_from_txt(txt_file_path)

    # Create a blank consolidated file before adding news articles
    with open(consolidated_file_path, 'w', encoding='utf-8') as f:
        f.write("Consolidated News\n")
        f.write("="*80 + "\n\n")
    # Store processed URLs to avoid repetition
    processed_urls = set()

    # Iterate over each Google News link and extract the real content of the news article
    for link in links:
        print(f"Processing link: {link}")
        real_url = get_real_url(link)
        if real_url and real_url not in processed_urls:  # Avoid duplicate URLs
            print(f"Processed real URL: {real_url}")
            title, date, content = extract_news_content(real_url)
            if content:
                print(f"\n### Title: {title}\n### Date: {date}\n### Content:\n{content[:500]}...\n")
                save_to_consolidated_file(title, date, content, consolidated_file_path)
                processed_urls.add(real_url)  # Mark the URL as processed
            else:
                print(f"Could not extract news content for {real_url}\n")
        else:
            print(f"Could not obtain the real URL for {link} or the URL has already been processed\n")

    print(f"Consolidated file saved at: {consolidated_file_path}")
    

# Example usage: replace with your actual paths
#execute_news_extraction_pipeline("/home/lourdes22/data-pipeline/files", "/home/lourdes22/data-pipeline/consolidated_files")
