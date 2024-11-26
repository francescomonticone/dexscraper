import os
import csv
import re
import requests
from datetime import datetime, timedelta
import logging
import time
from typing import List, Dict, Optional

class TwitterTokenScraper:
    """
    A comprehensive Twitter token mention scraper using Twitter API v2.
    
    Handles authentication, rate limiting, and extraction of token mentions 
    from specified Twitter list accounts.
    """
    
    def __init__(self, bearer_token: str, list_id: str):
        """
        Initialize the Twitter Token Scraper.
        
        Args:
            bearer_token (str): Twitter API Bearer Token
            list_id (str): Twitter List ID to scrape
        """
        self.bearer_token = bearer_token
        self.list_id = list_id
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            filename='twitter_scraper.log'
        )
        self.logger = logging.getLogger(__name__)
    
    def get_list_members(self) -> List[str]:
        """
        Fetch member IDs from the specified Twitter list.
        
        Returns:
            List of user IDs in the list
        """
        try:
            url = f"{self.base_url}/lists/{self.list_id}/members"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            members = response.json().get('data', [])
            return [member['id'] for member in members]
        
        except requests.RequestException as e:
            self.logger.error(f"Error fetching list members: {e}")
            return []
    
    def extract_token_mentions(self, text: str) -> List[Dict[str, str]]:
        """
        Extract token mentions from tweet text.
        
        Args:
            text (str): Tweet text to analyze
        
        Returns:
            List of dictionaries containing token mentions and contexts
        """
        # Regex to match token mentions like $BTC, $ETH, etc.
        token_pattern = r'\$([A-Z]{3,10})\b'
        
        mentions = []
        for match in re.finditer(token_pattern, text):
            token = match.group(1)
            
            # Extract narrative context (20 words around the token)
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end].strip()
            
            mentions.append({
                'token': token,
                'context': context
            })
        
        return mentions
    
    def get_user_tweets(self, user_id: str, days: int = 7) -> List[Dict]:
        """
        Retrieve recent tweets for a specific user.
        
        Args:
            user_id (str): Twitter user ID
            days (int): Number of days to look back
        
        Returns:
            List of tweets with token mentions
        """
        start_time = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"
        
        params = {
            'max_results': 100,
            'tweet.fields': 'created_at,text',
            'start_time': start_time
        }
        
        try:
            url = f"{self.base_url}/users/{user_id}/tweets"
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            tweets_data = response.json().get('data', [])
            
            token_tweets = []
            for tweet in tweets_data:
                token_mentions = self.extract_token_mentions(tweet['text'])
                
                if token_mentions:
                    for mention in token_mentions:
                        token_tweets.append({
                            'user_id': user_id,
                            'tweet_id': tweet['id'],
                            'created_at': tweet['created_at'],
                            'token': mention['token'],
                            'narrative': mention['context']
                        })
            
            return token_tweets
        
        except requests.RequestException as e:
            self.logger.error(f"Error fetching tweets for user {user_id}: {e}")
            return []
    
    def scrape_list_tokens(self, output_file: str = 'token_mentions.csv') -> None:
        """
        Scrape token mentions from all users in the list.
        
        Args:
            output_file (str): Path to save CSV output
        """
        # Fetch list members
        list_members = self.get_list_members()
        
        # Collect all token mentions
        all_token_mentions = []
        
        for user_id in list_members:
            self.logger.info(f"Scraping tweets for user ID: {user_id}")
            
            # Respect rate limits
            time.sleep(1)  # Basic rate limiting
            
            user_tweets = self.get_user_tweets(user_id)
            all_token_mentions.extend(user_tweets)
        
        # Write to CSV
        if all_token_mentions:
            keys = all_token_mentions[0].keys()
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                dict_writer = csv.DictWriter(csvfile, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(all_token_mentions)
            
            self.logger.info(f"Token mentions saved to {output_file}")
        else:
            self.logger.warning("No token mentions found.")

def main():
    # Configure your Twitter API credentials
    BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', 'your_bearer_token_here')
    LIST_ID = os.getenv('TWITTER_LIST_ID', 'your_list_id_here')
    
    # Initialize and run scraper
    scraper = TwitterTokenScraper(BEARER_TOKEN, LIST_ID)
    scraper.scrape_list_tokens()

if __name__ == "__main__":
    main()
