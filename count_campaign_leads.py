#!/usr/bin/env python3
"""Count pending campaign leads for auto-detect mode."""
import yaml
from pyairtable import Api

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

api = Api(config['airtable']['api_key'])
table = api.table(config['airtable']['base_id'], 'Campaign Leads')

leads = [r for r in table.all()
         if r['fields'].get('Enrich Lead')
         and not r['fields'].get('Linked Lead')
         and not r['fields'].get('Processing Notes', '').startswith(('EXCLUDED', 'PRE-'))]

print(len(leads))
