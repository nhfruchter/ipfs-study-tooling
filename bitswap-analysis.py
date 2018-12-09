from ipfs_logparse import Bitswap
import csv

b = Bitswap("logs/all-logs/bitswap.log")
b.parse()


# Produce high level summary 
summary = []
for event in b.output:
	summary.append({
		'ts': event['ts'],
		'wantlist_size': len(event['wantlist']),
		'partners_size': len(event['partners']),
		'blocks_recv': event['brecv'],
		'blocks_sent': event['bsent'],
		'blocks_dup': event['bdup']
	})

print("Writing summary to CSV.")
with open('bitswap.summary.csv', 'w') as f:
	header = ('ts', 'wantlist_size', 'partners_size', 'blocks_recv', 'blocks_sent', 'blocks_dup')
	w = csv.DictWriter(f, header)
	w.writeheader()
	w.writerows(summary)