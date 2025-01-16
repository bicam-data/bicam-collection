# for bicam

import time
import requests
from bs4 import BeautifulSoup
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import argparse
import random

from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
load_dotenv()

# Database connection parameters from .env
DB_HOST = os.getenv('POSTGRESQL_HOST')
DB_NAME = os.getenv('POSTGRESQL_DB')
DB_USER = os.getenv('POSTGRESQL_USER')
DB_PASSWORD = os.getenv('POSTGRESQL_PASSWORD')
DB_PORT = os.getenv('POSTGRESQL_PORT')


def create_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def get_data(query):
    conn = create_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        detection_data = cur.fetchall()
        df = pd.DataFrame(detection_data)
    conn.close()
    logger.info("Retrieved data from the database")
    return df

def scrape_text(url, max_retries=5, base_delay=1):
    # Expanded list of user agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0.0.0',
    ]
    
    # Common headers that can help avoid 403s
    base_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    if not url:
        return ""
    
    session = requests.Session()  # Create one session to reuse
    
    # First quick attempt with minimal headers
    try:
        headers = {'User-Agent': random.choice(user_agents)}
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser').get_text()
    except:
        pass
    
    # If first attempt failed, try with full headers
    for attempt in range(max_retries):
        try:
            headers = base_headers.copy()
            headers['User-Agent'] = user_agents[attempt % len(user_agents)]
            
            # Only add delay after first failure
            if attempt > 0:
                time.sleep(base_delay * (1.5 ** attempt))
            
            response = session.get(
                url,
                headers=headers,
                timeout=10,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                return BeautifulSoup(response.content, 'html.parser').get_text()
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_time = min(int(retry_after), 30)  # Cap wait time at 30 seconds
                        time.sleep(wait_time)
                        continue
                    except ValueError:
                        pass
            
            if response.status_code == 403:
                continue  # Try next user agent immediately
                
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to scrape {url} after {max_retries} attempts: {e}")
                return ""
    
    return ""

def get_processed_pairs(progress_file):
    if not os.path.exists(progress_file):
        return set()
    df = pd.read_csv(progress_file)
    # Create tuples of (id, url) pairs and return as a set
    return set(zip(df['id'].astype(str), df['url'].astype(str)))

def mark_as_processed(id_value, url, progress_file):
    df = pd.DataFrame({
        'id': [str(id_value)],
        'url': [str(url)]
    })
    if os.path.exists(progress_file):
        df.to_csv(progress_file, mode='a', header=False, index=False)
    else:
        df.to_csv(progress_file, index=False)

def save_text_to_csv(id_label, id, url, text, output_file=None):
    if output_file is None:
        output_file = f'scraped_{id_label}s.csv'
    
    df = pd.DataFrame({
        id_label: [id],
        'url': [url],
        'text': [text]
    })
    
    if os.path.exists(output_file):
        df.to_csv(output_file, mode='a', header=False, index=False, 
                 escapechar='\\', doublequote=True, quoting=1)
    else:
        df.to_csv(output_file, index=False, 
                 escapechar='\\', doublequote=True, quoting=1)
    
    logger.info(f"Saved text for {id_label} {id} to {output_file}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape text from URLs")
    parser.add_argument("--id_label", type=str, required=True, help="The label of the ID column")
    parser.add_argument("--url_column", type=str, required=True, help="The column containing the URLs")
    parser.add_argument("--query", type=str, required=True, help="The query to run to get the URLs")
    args = parser.parse_args()
    
    # Define progress file name
    progress_file = f'progress_{args.id_label}s.csv'
    
    # Get already processed ID-URL pairs
    processed_pairs = get_processed_pairs(progress_file)
    
    # Get all data from database
    df = get_data(args.query)
    
    # Convert IDs to strings for comparison
    df[args.id_label] = df[args.id_label].astype(str)
    df[args.url_column] = df[args.url_column].astype(str)
    
    # Create current pairs and filter out already processed ones
    current_pairs = set(zip(df[args.id_label], df[args.url_column]))
    pairs_to_process = current_pairs - processed_pairs
    
    # Convert pairs back to dataframe for processing
    df_to_process = pd.DataFrame(list(pairs_to_process), columns=[args.id_label, args.url_column])
    
    logger.info(f"Found {len(df_to_process)} new items to process out of {len(df)} total items")
    
    for _, row in tqdm(df_to_process.iterrows(), total=len(df_to_process), desc="Processing rows"):
        url = row[args.url_column]
        id = row[args.id_label]
        text = scrape_text(url)
        save_text_to_csv(args.id_label, id, url, text)
        mark_as_processed(id, url, progress_file)
