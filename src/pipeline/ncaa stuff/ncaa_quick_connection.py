"""
Quick test to diagnose NCAA endpoint issues
"""

import requests
import time

def test_endpoints():
    """Test different NCAA endpoints to see which work."""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.ncaa.com/'
    }
    
    # Test different endpoints
    test_urls = [
        # GraphQL endpoint (we know this works)
        {
            'name': 'GraphQL Games List',
            'url': 'https://sdataprod.ncaa.com/?meta=GetContests_web&extensions={"persistedQuery":{"version":1,"sha256Hash":"c1bd3e9f56889ebca2937ecf24a2d62ccbe771939687b5ef258a51a2110c1d57"}}&queryName=GetContests_web&variables={"sportCode":"MFB","division":3,"seasonYear":2024,"contestDate":null,"week":9}'
        },
        # Team stats endpoint
        {
            'name': 'Team Stats (casablanca)',
            'url': 'https://data.ncaa.com/casablanca/game/6308650/teamStats.json'
        },
        # Try without .json
        {
            'name': 'Team Stats (no .json)',
            'url': 'https://data.ncaa.com/casablanca/game/6308650/teamStats'
        },
        # Try boxscore endpoint
        {
            'name': 'Box Score',
            'url': 'https://data.ncaa.com/casablanca/game/6308650/boxscore.json'
        },
        # Try different game ID
        {
            'name': 'Team Stats (different game)',
            'url': 'https://data.ncaa.com/casablanca/game/6309059/teamStats.json'
        }
    ]
    
    for test in test_urls:
        print(f"\nTesting: {test['name']}")
        print(f"URL: {test['url'][:100]}...")
        
        try:
            start = time.time()
            # Add timeout to prevent hanging
            response = requests.get(test['url'], headers=headers, timeout=10)
            elapsed = time.time() - start
            
            print(f"Status: {response.status_code}")
            print(f"Time: {elapsed:.2f}s")
            
            if response.status_code == 200:
                print(f"Content-Type: {response.headers.get('content-type', 'Unknown')}")
                print(f"Content Length: {len(response.text)} characters")
                
                # Try to parse JSON
                try:
                    data = response.json()
                    print(f"JSON Valid: Yes")
                    print(f"Top-level keys: {list(data.keys())[:5]}")
                except:
                    print(f"JSON Valid: No")
            else:
                print(f"Error response: {response.text[:200]}")
                
        except requests.Timeout:
            print("TIMEOUT - Request took too long")
        except Exception as e:
            print(f"ERROR: {type(e).__name__}: {e}")
        
        print("-" * 60)


def test_with_curl_command():
    """
    Generate curl commands to test outside Python.
    """
    print("\n\nTry these curl commands in your terminal:")
    print("="*60)
    
    print("\n1. Team Stats endpoint:")
    print('curl -H "User-Agent: Mozilla/5.0" "https://data.ncaa.com/casablanca/game/6308650/teamStats.json"')
    
    print("\n2. With more headers:")
    print('curl -H "User-Agent: Mozilla/5.0" -H "Accept: application/json" -H "Referer: https://www.ncaa.com/" "https://data.ncaa.com/casablanca/game/6308650/teamStats.json"')


if __name__ == "__main__":
    print("NCAA Endpoint Connection Test")
    print("="*60)
    
    test_endpoints()
    test_with_curl_command()