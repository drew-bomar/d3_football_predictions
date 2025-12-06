from src.pipeline.ncaa_api_client import NCAAAPIClient
import json
import requests

# Make the raw request directly
variables = {
    'contestId': '6461884',
    'staticTestEnv': None
}

extensions = {
    'persistedQuery': {
        'version': 1,
        'sha256Hash': 'b41348ee662d9236483167395b16bb6ab36b12e2908ef6cd767685ea8a2f59bd'
    }
}

params = {
    'meta': 'NCAA_GetGamecenterTeamStatsFootballById_web',
    'extensions': json.dumps(extensions, separators=(',', ':')),
    'variables': json.dumps(variables, separators=(',', ':'))
}

response = requests.get('https://sdataprod.ncaa.com/', params=params)
data = response.json()

print('Response status:', response.status_code)
print('Response keys:', list(data.keys()))

if 'data' in data:
    print('Data keys:', list(data['data'].keys()))
    boxscore = data['data'].get('boxscore')
    print('Boxscore type:', type(boxscore))
    print('Boxscore is None:', boxscore is None)
    
    if boxscore:
        print('Has teamBoxscore:', 'teamBoxscore' in boxscore)
        if 'teamBoxscore' in boxscore:
            print('Number of teams:', len(boxscore['teamBoxscore']))
    else:
        print('Boxscore is empty or None')
else:
    print('No data key in response')
