from datetime import datetime, date
import os
from multiaddr import Multiaddr
from tqdm import tqdm

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def regularize_bytes(amount):
	# Assumed input: tuple of ("##", "xB/s")
	quantity, unit = amount
	quantity = float(quantity)
	unit = unit.replace("/s", "").lower().strip()
	
	multipliers = {
		"b": 10**0,
		"kb": 10**3,
		"mb": 10**6,
		"gb": 10**9
	}
	return quantity * multipliers[unit]
	
class LogParser(object):
	def __init__(self, filename, prefix="::", delimiter="===___END___==="):
		self.fn = filename
		self.delimiter = delimiter
		self.prefix = prefix
		self.records = []
		self.raw = ""
	
	def parse(self):
		raise NotImplementedError
		
	def _split_delim(self):
		self.records = [r.strip() for r in self.raw.split(self.prefix + self.delimiter)]

	def _split_ts(self):
		self.records = [r.strip() for r in self.raw.split(self.prefix + " ")]		

class OpenPeers(LogParser):
	def __init__(self, filename):
		super(OpenPeers, self).__init__(filename)	
		self.output = []				
	
	def parse(self):
		with open(self.fn) as f:
			self.raw = f.read()
			self._split_delim()

		self.records = [r.split("\n") for r in self.records if len(r) > 2]
		self.records = [r for r in self.records if len(r) > 1]	
		
		for doc in tqdm(self.records):
			_ts = doc.pop(0) 
			doc.pop(0)
			_ts = int(_ts.replace(self.prefix, "").strip())
			_ts = datetime.fromtimestamp(_ts)
			
			parsed_doc = {'ts': _ts, 'peers': []}
			for line in doc:
				if "p2p-circuit" not in line:
					addr, peer = line.split("/ipfs/")
					this = {peer: Multiaddr(addr)}					
				else:
					line = line.split("/")
					relay = line[1:3]
					destination = line[4:]
					this = {'p2p-circuit': (relay, destination)}					
					
				parsed_doc['peers'].append(this)

			self.output.append(parsed_doc)	
			
	def to_json(self, name="peers.json"):			
		with open(name, 'w') as f:
			json.dump(self.output, f, default=json_serial)
			
		print("Exported to {name}".format(name=name))	
			
				
		
class Bitswap(LogParser):		
	# TODO
	def __init__(self, filename):
		super(Bitswap, self).__init__(filename)
		self.output = []
		
	def parse(self):
		def find(l, k):
			return [ i for i,_ in enumerate(l) if k in _ ][0]                                                                                                                           
			
		with open(self.fn) as f:
			self.raw = f.read()
			self._split_delim()

		self.records = [r.split("\n") for r in self.records if len(r) > 2]
		self.records = [r for r in self.records if len(r) > 1]	
		
		for r in tqdm(self.records):
			_ts = r.pop(0) 
			r.pop(0)
			_ts = int(_ts.replace(self.prefix, "").strip())
			_ts = datetime.fromtimestamp(_ts)
			
			# Find index of "wantlist"
			i_wantlist = find(r, "wantlist")
			i_partners = find(r, "partners")
			i_brecv = find(r, "blocks received")
			i_bsent = find(r, "blocks sent")
			i_recv = find(r, "data received")
			i_sent = find(r, "data sent")
			i_bdup = find(r, "dup blocks")
			i_dup = find(r, "dup data")
			
			this = {
				'ts': _ts,
				'wantlist': [line.strip() for line in r[i_wantlist+1:i_partners]],
				'partners': [line.strip() for line in r[i_partners+1:]],
				'brecv': r[i_brecv].split(": ")[1],
				'bsent': r[i_bsent].split(": ")[1],
				'recv': r[i_recv].split(": ")[1],
				'sent': r[i_sent].split(": ")[1],
				'bdup': r[i_bdup].split(": ")[1],
				'dup': r[i_dup].split(": ")[1]
			}
			self.output.append(this)
			
	def to_json(self, name="bitswap.json"):			
		with open(name, 'w') as f:
			json.dump(self.output, f, default=json_serial)
		
		print("Exported to {name}".format(name=name))				
		
class KnownPeers(LogParser):
	def __init__(self, filename):
		super(KnownPeers, self).__init__(filename)
		self.output = []				
		self.IP_RESERVED = ["/ip6/::1", "/ip4/192.168", "/ip4/127.", "/ip4/172.", "/ip4/10.", "/ip4/169.254", "/ipfs", '/ip4/198.18', '/ip4/198.19', '/ip4/100.']
		
	def parse(self):		
		def chunk(l):
			output = {}
			this = []
			for line in l:
				if line.startswith("Q") or "(" in line:
					if len(this):
						peer = this[0].split(" ")[0]
						output[peer] = this[1:]
						this = []	
						this.append(line)
					else:	
						this.append(line)
				else:
					this.append(line.strip())
					
			return output										
		
		print("Loading file...")	
		with open(self.fn) as f:
			self.raw = f.read()
			self._split_ts()

		print("Cleaning records...")
		self.records = [r.split("\n") for r in self.records if len(r) > 2]
		self.records = [r for r in self.records if len(r) > 1]	

		print("Parsing records...")	
		chunked = []
		for r in tqdm(self.records):

			_ts = r.pop(0) 
			_ts = int(_ts.replace(self.prefix, "").strip())
			_ts = datetime.fromtimestamp(_ts)
			
			r.pop(0) # Remove trailing newline
			record = {
				'ts': _ts,
				'peers': None
			}
			peers = chunk(r)
			_peers = {}
			for k, v in peers.items():
				multi = []
				for thisaddr in v:
					if any(part in thisaddr for part in self.IP_RESERVED):
						# Skip local IP announcements
						continue
					try:
						multi.append(Multiaddr(thisaddr))
					except:
						multi.append(thisaddr)
					
				peerObj = {
					'multiaddr': multi,
					'ips': [],
					'locations': []						
				}	
				for a in multi:
					try:
						peerObj['ips'].append(str(a).split("/")[2])
					except:
						continue	
				_peers[k] = peerObj
				
			record['peers'] = _peers
						
			chunked.append(record)

		self.output = chunked
		print("Done")
		
	def to_json(self, name="peers.json"):			
		with open(name, 'w') as f:
			json.dump(self.output, f, default=json_serial)
			
		print("Exported to {name}".format(name=name))	
				
						
class Bandwidth(LogParser):
	def __init__(self, filename, bwProto='cumulative'):
		super(Bandwidth, self).__init__(filename)
		self.output = []				
		self.protocol = os.path.basename(filename).replace("bandwidth", "").replace(".log", "") or "cumulative"

	def parse(self):
		def _clean_bw(s, marker):
			return regularize_bytes(s.replace("{m}: ".format(m=marker), "").strip().split())
		
		with open(self.fn) as f:
			self.raw = f.read()
			self._split_delim()

		self.records = [r.split("\n") for r in self.records if len(r) > 2]
		self.records = [r for r in self.records if len(r) > 1]	
		
		for _ts, _, __, _tin, _tout, _rin, _rout in self.records:
			_ts = int(_ts.replace(self.prefix, "").strip())
			_ts = datetime.fromtimestamp(_ts)
			
			_tin = _clean_bw(_tin, "TotalIn")
			_tout = _clean_bw(_tout, "TotalOut")
			_rin = _clean_bw(_rin, "RateIn")
			_rout = _clean_bw(_rout, "RateOut")
			
			parsedRecord = {
				'timestamp': _ts,
				'protocol': self.protocol,
				'total_down': _tin,
				'total_up': _tout,
				'rate_in': _rin,
				'rate_out': _rout,
			}			
			self.output.append(parsedRecord)
			
	def to_csv(self, name=""):
		import csv
		
		fields = ('protocol', 'timestamp', 'total_down', 'total_up', 'rate_in', 'rate_out') 
		if not name:
			name = "bandwidth.{proto}.csv".format(proto=self.protocol)

		with open(name, 'w') as f:
			writer = csv.DictWriter(f, fieldnames=fields)
			writer.writeheader()
			writer.writerows(self.output)
		
		print("Exported to {name}".format(name=name))	
		
	def to_json(self, name=""):
		if not name:
			name = "bandwidth.{proto}.json".format(proto=self.protocol)
			
		with open(name, 'w') as f:
			json.dump(self.output, f, default=json_serial)
			
		print("Exported to {name}".format(name=name))	