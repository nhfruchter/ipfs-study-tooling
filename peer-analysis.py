import itertools
import geoip2.database
import csv
import pickle

import requests
from ipfs_logparse import *

mm_asn = geoip2.database.Reader("GeoLite2-ASN.mmdb")
mm_city = geoip2.database.Reader("GeoLite2-City.mmdb")

def ipinfo(ip):
	"""Use MaxMind databases to look up info about an IP."""
	asn = mm_asn.asn(ip)
	loc = mm_city.city(ip)

	return {
		'asn': asn.autonomous_system_number,
		'aso': asn.autonomous_system_organization,
		'lat': loc.location.latitude,
		'lon': loc.location.longitude,
		'continent': loc.continent.code,
		'country': loc.country.iso_code,
		'city': loc.city.name
	}

def ipinfo_remote(ip):
	"""Use the ipinfo.io API to look up info about an API. """
	token = "e3a2dd300c22ac"
	return requests.get(
		"http://ipinfo.io/{ip}?token={tok}".format(ip=ip, tok=token)
	).json()
	
def generate_peerlist(fn):
	"""Generate mapping of peer hash to multiaddr IPs."""
	
	print("Opening file {0}".format(fn))
	
	kp = KnownPeers(fn)
	kp.parse()
		
	print("Creating peer hash to IP mapping")
	peers = []
	for doc in tqdm(kp.output): 
		ts = doc['ts'] 
		ips = [ {peer: set(pobj['ips'])} for peer,pobj in doc['peers'].items()]  
		peers.append( {'ts': ts, 'ips': ips} ) 

	ips = [[list(p.values()) for p in record['ips']] for record in peers]
	out = itertools.chain.from_iterable( itertools.chain.from_iterable( itertools.chain.from_iterable(ips) ) )
	
	return peers, ips, out
	
def unique_ips_seen(peers):
	"""Generate list of unique IPs seen."""

	# TS unique IPs
	print("Creating list of unique IPs")
	ips = [[list(p.values()) for p in record['ips']] for record in peers]
	lens = [list(itertools.chain.from_iterable( itertools.chain.from_iterable(i) )) for i in ips]
	finalLens = [(len(i), len(set(i))) for i in lens]
	finalLens = [(a,b,a-b) for a,b in finalLens]
	ts = [record['ts'] for record in peers]
	unique = [(x,) + y for x, y in zip(ts, finalLens)]
	unique[0] = ('ts', 'total', 'unique', 'delta')

	print("Writing CSV file")
	with open("timeseries-unique-peers.{0}.csv".format(fn), "w") as f:
	    w = csv.writer(f)
	    w.writerows(unique)

def make_ipmap(peers):
	"""Load existing IP address mapping. """
	
	# Load existing
	print("Loading existing IP mapping")
	with open(IPMAP_FN, 'rb') as f: 
		lookup = pickle.load(f) 

	# Get unique IPs from logs
	print("Parsing log IPs")
	unique_ips = set()
	for doc in tqdm(peers):
		for p in doc['ips']:
			for ipset in p.values(): unique_ips.update(ipset)
		
	# Use MaxMind to find IPs
	not_in_lookup = list(set(lookup.keys()) - unique_ips)
	errors = []
	for ip in tqdm(not_in_lookup):
		try:
			lookup[ip] = ipinfo(ip)		
		except:
			errors.append(ip)

	# Use ipinfo.io to look up remaining
	print("Using ipinfo for remaining IPs")
	ipinfo_results = []
	for ip in tqdm(errors):
		ipinfo_results.append(ipinfo_remote(ip))
	ipinfo_results = [i for i in ipinfo_results if not (len(i) == 2 and not i.get('org'))]                           
	for obj in ipinfo_results:
		_asn = obj.get('org')
		if _asn:
			_asn = asn.split(" ")
			asn = _asn[0].replace("AS", "")
			aso = " ".join(_asn[1:])		
		else:
			if obj.get('hostname') and 'northamericancoax' in obj.get('hostname'):
				aso = "Private Internet Access"
			else:		
				asn, aso = "", ""
		_loc = obj.get('loc')

		if _loc:
			_loc = _loc.split(",")
			lat, lon = _loc	
		else:
			lat, lon = "", ""
	
		city = obj.get('city') or ""
		country = obj.get('country') or ""
		ip = obj.get('ip')
	
		lookup[ip] =  {
			'asn': asn, 'aso': aso,
			'lat': lat, 'lon': lon,
			'continent': "", 'country': country, 'city': city
		}

	# Save mapping
	print("Saving new IP mapping")
	with open(IPMAP_FN, 'wb') as f: 
		pickle.dump(lookup, f)

	return lookup 

def create_peer_timeseries(peers, lookup):
	"""Create a timeseries of peers seen at each measurement interval."""
	
	print("Creating timeseries of peers seen.")	
	timeseries_ips = []
	for timestep in tqdm(peers):
		ts = timestep['ts']
		for peer in timestep['ips']:
			peer_ips = itertools.chain.from_iterable(peer.values())
			for ip in peer_ips:
				geo = lookup.get(ip) 
				if not geo: continue
				row = {'ts': ts}
				row.update(geo)
				timeseries_ips.append(row)
				
	print("Saving timeseries")			
	with open("timeseries-peer-geo.{0}.csv".format(fn), 'w') as f:
		header = ('ts', 'asn', 'aso', 'lat', 'lon', 'continent', 'country', 'city')
		w = csv.DictWriter(f, header)
		w.writeheader()
		w.writerows(timeseries_ips)
if __name__ == '__main__':
	
	# List of filenames that contain known.peers.log content. 
	KNOWNPEER_LOGS = []
	IPMAP_FN = "/Users/nhf/Projects/6s974/ipfs/results/ipmapping.pkl"
	
	if not len(KNOWNPEER_LOGS): 
		raise ValueError("Please specify at least one file to parse.")

	for fn in KNOWNPEER_LOGS:
		peers, ips, out = generate_peerlist(fn)
		unique_ips_seen(peers)
		lookup = make_ipmap(peers)
		create_peer_timeseries(peers, lookup)





	
