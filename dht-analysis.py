import jsonlines
import csv
import numpy as np
import pickle
from tqdm import tqdm
import pandas as pd

import locale
locale.setlocale
locale.setlocale(locale.LC_NUMERIC, '')

def fstat(h, timeout=1):
	try:
		info = requests.get('http://127.0.0.1:5001/api/v0/files/stat?arg=/ipfs/{0}'.format(h), timeout=timeout).json()
	except:
		info = {'Hash': h, 'timeout': True} 
	return info	
	
########

FN = "dht.all.log"

# Read file
print("Reading DHT logs")
dht = jsonlines.open(FN).iter(skip_invalid=True) 

# Parse DHT event stream, discarding any info that isn't useful to us
print("Parsing event stream")
for e in tqdm(dht):
    ts = e['Start'].split(".")[0]
    op = e['Operation']
    if e.get('Tags'):
        hash = e['Tags'].get('key') or ''
    else:
        hash = ''
    out.append( {'ts': ts, 'op': op, 'hash': hash} )

# Write as timeseries CSV
with open("dht-events.csv", 'w') as f:
    w = csv.DictWriter(f, ('ts', 'op', 'hash'))
    w.writeheader()
    w.writerows(out)

# Analyses of activity by time bin

df = pd.DataFrame.from_dict(out)

# Index by time
pd.to_datetime(df.ts)
df['dti'] = pd.DatetimeIndex(df['ts'])
df.index = df.dti

# Operations by hour and day
df.groupby(df.index.hour)['op'].value_counts()
ops_by_hour = df.groupby(df.index.hour)['op'].value_counts()
ops_by_hour.to_csv('ops-by-hour.csv')

# Create sample of file hashes for closer examination
hashes = np.random.choice(x, size=round(len(x)*0.05), replace=False)
with open("hashes-sample.txt", "w") as f: f.write("\n".join(hashes))

# Scrape hashes: retry with 1, 10, 30second timeout
print("Scraping {0} hashes with timeout 1".format(len(hashes)))
hashMetadata, timeouts = [], set()
for e in tqdm(hashes):
	result = fstat(e['Hash'])
	if 'timeout' in result:
		timeouts.add( e["Hash"]) 
	else:
		hashMetadata.append(result)

timeouts10 = set()	
print("{n} timeouts, retrying timeout 10".format(len(timeouts)))
for hash in tqdm(timeouts):
	result = fstat(hash, timeout=10)
	if 'timeout' in result:
		timeouts10.add( hash)
	else:
		hashMetadata.append(result)
		
print("{n} timeouts, final retry with timeout 30".format(len(timeouts10)))
timeouts30 = set()
for hash in tqdm(timeouts10):
	result = fstat(hash, timeout=30)
	if 'timeout' in result:
		timeouts30.add( hash)
	else:
		hashMetadata.append(result)

print("Data on {n} hashes, {m} timed out after 30 seconds".format(n=len(hashMetadata), m=len(timeouts30)))

# Finally, add any unscrapable hashes to the metadata set
for hash in timeouts30:
	hashMetadata.append({
		'Hash': hash,
		'timeout': 30,
	})

# Finally, scrape metadata about folders that we found
# Hashes that correspond to folders
folders = set(r.get('Hash') for r in hashMetadata if r.get('Type') == 'directory')                  print("Finding out about the folders seen: {0} folders".format(len(folders)))
dirErrors, dirTimeouts = set(), set()
directoryMeta = []

print("Scraping directory listing with timeout 10")
for h in tqdm(folders):
	try:
		result = requests.get("http://127.0.0.1:5001/api/v0/ls?arg={0}".format(h), timeout=10).json()
	except:
		result = None
		
	if result:
		actualResult = result.get('Objects')
		if actualResult:
			directoryMeta.append(actualResult[0])	
		else:
			dirErrors.add(h)	
	else:
		dirTimeouts.add(h)

dirTimeouts30, dirErrors30 = set(), set()
print("Retrying {0} folders with timeout 30".format(len(dirTimeouts)))
for h in tqdm(dirTimeouts):
	try:
		result = requests.get("http://127.0.0.1:5001/api/v0/ls?arg={0}".format(h), timeout=30).json()
	except:
		result = None
		
	if result:
		actualResult = result.get('Objects')
		if actualResult:
			directoryMeta.append(actualResult[0])	
		else:
			dirErrors30.add(h)	
	else:
		dirTimeouts30.add(h)

print("Saving results to directory-meta.pkl pickle")
with open('directory-meta.pkl', 'wb') as f:
	pickle.dump([directoryMeta, timeouts], f)

