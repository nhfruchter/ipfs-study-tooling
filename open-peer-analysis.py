from ipfs_logparse import OpenPeers
import csv
import pickle

FN = "/Users/nhf/Projects/6s974/ipfs/logs/all-logs/open.peers.log"
IPMAP_FN = "/Users/nhf/Projects/6s974/ipfs/results/ipmapping.pkl"

###

print("Loading open peers log...")
op = OpenPeers(FN)
op.parse()

print("Generating summary...")
# Summarize output: first do simple counts
summary = []
for event in op.output:
	summary.append({
		'ts': event['ts'],
		'peers_size': len(event['peers'])
	})
	
print("Counting protocols used...")
for i,timestep in tqdm(enumerate(op.output), total=len(op.output)):
	this_timestep_protos = []
	for peer in timestep['peers']:
		addrs = peer.values()
		for addr in addrs:
			if isinstance(addr, tuple): continue
			this_peer_protos = [p.name for p in addr.protocols()] 
			this_timestep_protos += this_peer_protos
	
	this_timestep_protos = dict(Counter(this_timestep_protos))		
	summary[i].update(this_timestep_protos)

print("Updating IP mapping with open peers not seen known peers.")
new_ips = set()
for i,timestep in tqdm(enumerate(op.output), total=len(op.output)):
	for peer in timestep['peers']:
		addrs = peer.values()
		for addr in addrs:
			try:
				this_ip = addr.value_for_protocol(4)
				if this_ip not in lookup:
					new_ips.add(this_ip)
			except:
				continue	
				
import geoip2.database
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

errors = []
for ip in tqdm(new_ips):
	try:
		lookup[ip] = ipinfo(ip)		
	except:
		errors.append(ip)
		

for i,timestep in tqdm(enumerate(op.output), total=len(op.output)):
	counts = {
		'asn': [],
		'country': [],
		'city': [],
		'aso': []
	}
	for peer in timestep['peers']:
		addrs = peer.values()
		for addr in addrs:
			if isinstance(addr, tuple): continue
			this_ip = addr.value_for_protocol(4)	
			this_ip = lookup.get(this_ip)
			if this_ip:
				for key in ('asn', 'aso', 'country', 'city'):
					value = this_ip.get(key)
					if isinstance(value, list): 
						value = " ".join(value)
					counts[key].append(value)

	counts['asn'] = Counter(counts['asn'])
	counts['aso'] = Counter(counts['aso'])
	counts['city'] = Counter(counts['city'])
	counts['country'] = Counter(counts['country'])
	
	summary[i]['ip_counters'] = counts

with open("active-peers.summary.pkl", 'wb') as f:
	pickle.dump(summary, f)
	
# For graph production see the iPython notebook