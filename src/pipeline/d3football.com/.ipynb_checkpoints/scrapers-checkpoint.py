"""
D3 Football Data Scrapers
Handles all web scraping operations for d3football.com

Key Functions:
- scrape_week(): Get basic game results for a week
- parse_complete_box_score(): Extract detailed stats from box score pages
- scrape_week_with_box_scores(): Combined function for complete week data
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import json
from pathlib import Path
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class D3FootballScraper:
    """
    Streamlined scraper for d3football.com that discovers box score URLs 
    directly from scoreboard pages and extracts complete game data in one operation.
    
    Attributes:
        base_url (str): Base URL for d3football.com
        delay (float): Rate limiting delay between requests in seconds
        headers (dict): HTTP headers for requests
        session (requests.Session): Persistent session for HTTP requests
    """

    def __init__(self, base_url="https://www.d3football.com", delay=1.5):
        self.base_url = base_url
        self.delay = delay  # Rate limiting delay between requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)


    def scrape_week_complete(self, year: int, week: int, max_games: int = None) -> dict:
        """
        Complete one-step week scraping that discovers box score URLs from the scoreboard
        and parses all box scores to extract detailed game statistics.
        
        Args:
            year (int): Season year (e.g., 2024)
            week (int): Week number (1-15)
            max_games (int, optional): Maximum number of games to process for testing. Defaults to None (all games).
        
        Output:
            dict: {
                'box_scores': List[Dict] - Detailed game data with team stats, scores, records
                'summary': Dict - Success statistics including total_games, successful_box_scores, 
                                failed_box_scores, success_rate
            }
        
        Helper Methods Called:
            - _discover_box_score_urls(): Finds all box score URLs for the week
            - parse_complete_box_score(): Extracts detailed stats from each box score
        """
        logger.info(f"Scraping complete week data: {year} Week {week}")
        
        # Step 1: Discover box score URLs from scoreboard
        box_score_urls = self._discover_box_score_urls(year, week)
        
        if not box_score_urls:
            logger.warning(f"No box score URLs found for {year} Week {week}")
            return {'box_scores': [], 'summary': {'total_games': 0, 'success_rate': 0}}
        
        # Limit for testing
        if max_games:
            box_score_urls = box_score_urls[:max_games]
            logger.info(f"Limited to {max_games} games for testing")
        
        # Step 2: Parse all box scores
        box_scores = []
        successful = 0
        
        for i, url in enumerate(box_score_urls):
            logger.info(f"Processing box score {i+1}/{len(box_score_urls)}: {url}")
            
            if i > 0:
                time.sleep(self.delay)  # Rate limiting
            
            box_score_data = self.parse_complete_box_score(url)
            
            # Add metadata
            box_score_data.update({
                'year': year,
                'week': week,
                'game_id': f"{year}_{week}_{i+1}"
            })
            
            box_scores.append(box_score_data)
            
            # Track success
            if not box_score_data.get('parsing_errors'):
                successful += 1
            else:
                logger.warning(f"Parsing errors for {url}: {box_score_data['parsing_errors']}")
        
        summary = {
            'total_games': len(box_score_urls),
            'successful_box_scores': successful,
            'failed_box_scores': len(box_score_urls) - successful,
            'success_rate': successful / len(box_score_urls) if box_score_urls else 0
        }
        
        logger.info(f"Week {week} complete: {successful}/{len(box_score_urls)} successful ({summary['success_rate']:.1%})")
        
        return {
            'box_scores': box_scores,
            'summary': summary
        }
    
    def _discover_box_score_urls(self, year: int, week: int) -> list[str]:
        """
        Extract box score URLs from the D3 football scoreboard page using the correct URL format.
        Based on testing: https://www.d3football.com/scoreboard/{year}/composite?view={week}
        
        Args:
            year (int): Season year (2022, 2023, 2024, etc.)
            week (int): Week number (1-15)
        
        Output:
            List[str]: List of relative box score URLs like ['/seasons/2024/boxscores/20240907_m7l7.xml', ...]
        
        Helper Methods Called:
            None (internal method)
        """
        
        # Use the working URL pattern from testing
        url = f"{self.base_url}/scoreboard/{year}/composite?view={week}"
        
        try:
            logger.info(f"Discovering box scores from: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            box_score_urls = []
            
            # Method 1: Look for BX links (primary method - most reliable)
            bx_links = soup.find_all('a', string='BX')
            logger.info(f"Found {len(bx_links)} BX links")
            
            for link in bx_links:
                if 'href' in link.attrs:
                    href = link['href']
                    box_score_urls.append(href)
            
            # Method 2: Backup - look for boxscore links if BX links fail
            if not box_score_urls:
                logger.warning(f"No BX links found, trying boxscore links as backup")
                boxscore_links = soup.find_all('a', href=lambda x: x and 'boxscore' in x and str(year) in x)
                
                for link in boxscore_links:
                    box_score_urls.append(link['href'])
                
                logger.info(f"Found {len(box_score_urls)} boxscore links as backup")
            
            # Remove duplicates while preserving order
            unique_urls = list(dict.fromkeys(box_score_urls))
            
            if unique_urls:
                logger.info(f"Successfully discovered {len(unique_urls)} unique box score URLs")
                return unique_urls
            else:
                logger.warning(f"No box score URLs found for {year} week {week}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"Error accessing {url}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error discovering URLs for {year} week {week}: {e}")
            return []

    def parse_complete_box_score(self, box_score_url: str) -> dict:
        """
        Extract comprehensive game statistics from a D3 football box score page including
        team names, detailed stats, final scores, and team records.
        
        Args:
            box_score_url (str): Relative or absolute URL to box score page
        
        Output:
            Dict: Complete game data containing:
                - box_score_url: Original URL
                - parsing_errors: List of any errors encountered
                - scraped_at: ISO timestamp of scraping
                - team1/team2: Team names
                - team1_*/team2_*: All statistical categories for both teams
                - team1_final_score/team2_final_score: Final scores
                - team1_record_*/team2_record_*: Team records before/after game
        
        Helper Methods Called:
            - _extract_box_score_data(): Performs actual HTML parsing and data extraction
        """
        if not box_score_url:
            return {'parsing_errors': ['No box score URL provided']}
        
        # Handle relative URLs
        if box_score_url.startswith('/'):
            full_url = self.base_url + box_score_url
        else:
            full_url = box_score_url
        
        logger.debug(f"Parsing box score: {box_score_url}")
        
        try:
            response = self.session.get(full_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            return self._extract_box_score_data(soup, box_score_url)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching box score {box_score_url}: {e}")
            return {'parsing_errors': [f'Request error: {str(e)}']}
        except Exception as e:
            logger.error(f"Error parsing box score {box_score_url}: {e}")
            return {'parsing_errors': [f'Parsing error: {str(e)}']}



    def _extract_box_score_data(self, soup, box_score_url):
            """
            Internal method that performs the actual HTML parsing to extract all game data
            from a BeautifulSoup object representing a box score page.
            
            Args:
                soup (BeautifulSoup): Parsed HTML of the box score page
                box_score_url (str): Original URL for reference
            
            Output:
                Dict: Complete game data dictionary with team stats, scores, records, and metadata
            
            Helper Methods Called:
                None (internal method - uses regex and BeautifulSoup operations)
            """
            game_data = {
                'box_score_url': box_score_url,
                'parsing_errors': [],
                'scraped_at': datetime.now().isoformat()
            }
    
            try:
                # PART 1: Extract stats from the main statistics table
                stats_table = None
                tables = soup.find_all('table', class_='all-center')
    
                # Look for stats table, it will always start with FIRST DOWNS
                for table in tables:
                    if 'FIRST DOWNS' in table.get_text():
                        stats_table = table 
            
                if not stats_table:
                    game_data['parsing_errors'].append('Stats table not found')
                    return game_data            
    
                # Get team names from header row
                header_row = stats_table.find('tr')
                header_cells = header_row.find_all('th')
                if len(header_cells) >= 3:
                    game_data['team1'] = header_cells[0].text.strip()
                    game_data['team2'] = header_cells[2].text.strip()
    
                # Get all statistics from stats table
                stats_rows = stats_table.find_all('tr')[1:]
    
                skip_next = False
                for row in stats_rows:
                    if skip_next:
                        skip_next = False
                        continue
                        
                    cells = row.find_all('td')
                    if len(cells) == 3:
                        # Get raw text with newlines preserved for third down parsing
                        value1_raw = cells[0].get_text('\n', strip=True)
                        stat_name_raw = cells[1].get_text('\n', strip=True)
                        value2_raw = cells[2].get_text('\n', strip=True)
    
                        # Check if this is a multi line cell
                        if '\n' in stat_name_raw:
                            # This cell has multiple stats - skip it for now
                            continue
                        
                        stat_name = stat_name_raw.strip()
    
                        # Skip subcategory rows (they don't have the stat name in proper format)
                        if stat_name in ['Passing', 'Rushing', 'Penalty', 'Average', 
                                       'Completions-Attempts', 'Rushing Attempts', 
                                       'Total Offensive Plays']:
                            continue
                        
                        # For FIRST DOWNS, skip the next row (subcategories)
                        if stat_name == 'FIRST DOWNS':
                            skip_next = True
    
                        # For most stats, use first line only
                        value1 = value1_raw.split('\n')[0].strip()
                        value2 = value2_raw.split('\n')[0].strip()
                        
                        # Store the stat
                        clean_stat_name = stat_name.lower().replace(' ', '_').replace(':', '')
                        game_data[f'team1_{clean_stat_name}'] = value1
                        game_data[f'team2_{clean_stat_name}'] = value2
                        
                        # SPECIAL HANDLING: For third down efficiency, store the full raw text
                        if 'THIRD DOWN EFFICIENCY' in stat_name:
                            game_data[f'team1_third_down_raw'] = value1_raw
                            game_data[f'team2_third_down_raw'] = value2_raw
    
                # PART 2: Parse third down efficiency (handles newlines)
                for team in ['team1', 'team2']:
                    raw_key = f'{team}_third_down_raw'
                    if raw_key in game_data:
                        td_text = game_data[raw_key]
                        
                        # Extract percentage
                        pct_match = re.search(r'(\d+)%', td_text)
                        if pct_match:
                            game_data[f'{team}_third_down_pct'] = pct_match.group(1)
                        
                        # Extract attempts
                        attempts_match = re.search(r'(\d+)\s+of\s+(\d+)', td_text)
                        if attempts_match:
                            conversions = attempts_match.group(1)
                            attempts = attempts_match.group(2)
                            game_data[f'{team}_third_down_conversions'] = conversions
                            game_data[f'{team}_third_down_att'] = attempts
    
                # PART 3: Parse fumbles
                for team in ['team1', 'team2']:
                    fumbles_key = f'{team}_fumbles_number-lost'
                    if fumbles_key in game_data:
                        fumbles_text = game_data[fumbles_key]
                        fumbles_clean = re.sub(r'\s+', '', fumbles_text)
                        if '-' in fumbles_clean:
                            parts = fumbles_clean.split('-')
                            game_data[f'{team}_fumbles'] = parts[0].strip()
                            game_data[f'{team}_fumbles_lost'] = parts[1].strip()
    
                # PART 4: Parse interceptions
                for team in ['team1', 'team2']:
                    int_key = f'{team}_interceptions_number-yards'
                    if int_key in game_data:
                        int_text = game_data[int_key]
                        if '-' in int_text:
                            parts = int_text.split('-')
                            game_data[f'{team}_interceptions'] = parts[0].strip()
                            game_data[f'{team}_interception_return_yards'] = parts[1].strip()
    
                # PART 5: Extract final scores
                all_tables = soup.find_all('table')
                
                for table in all_tables:
                    table_text = table.get_text()
                    
                    team1_name = game_data.get('team1', '')
                    team2_name = game_data.get('team2', '')
                    
                    if (team1_name in table_text and team2_name in table_text and
                        re.search(r'\b\d{1,2}\b', table_text)):
                        
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            for cell in cells:
                                cell_text = cell.get_text().strip()
                                
                                if team1_name in cell_text:
                                    score_match = re.search(r'\b(\d{1,2})\b', cell_text)
                                    if score_match:
                                        game_data['team1_final_score'] = score_match.group(1)
                                
                                elif team2_name in cell_text:
                                    score_match = re.search(r'\b(\d{1,2})\b', cell_text)
                                    if score_match:
                                        game_data['team2_final_score'] = score_match.group(1)
                        
                        if 'team1_final_score' in game_data and 'team2_final_score' in game_data:
                            break
                
                # PART 6: Extract team records
                page_text = soup.get_text()
                record_patterns = re.findall(r'\((\d+-\d+),\s*(\d+-\d+)\)', page_text)
                
                if len(record_patterns) >= 2:
                    team1_records = record_patterns[0]
                    team2_records = record_patterns[1]
                    
                    game_data['team1_record_after'] = team1_records[0]
                    game_data['team1_record_before'] = team1_records[1]
                    game_data['team2_record_after'] = team2_records[0] 
                    game_data['team2_record_before'] = team2_records[1]
                elif len(record_patterns) == 1:
                    game_data['parsing_errors'].append('Only found one team record pattern')
                                           
            except Exception as e:
                game_data['parsing_errors'].append(f'Error: {str(e)}')
    
            return game_data
