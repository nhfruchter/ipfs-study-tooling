#!/bin/bash

EOR="\n\n::===___END___===\n\n"
TIMESTAMP=":: $(date +%s)\n"

echo -e "Measuring at $TIMESTAMP"

FILE_BW="bandwidth.log"
FILE_BIT="bitswap.log"
FILE_PEERS="open.peers.log"
FILE_KNOWN="known.peers.log"

# See https://github.com/libp2p/specs/blob/master/7-properties.md#757-protocol-multicodecs
IPFS_PROTOS="/secio/1.0.0 /plaintext/1.0.0 /spdy/3.1.0 /yamux/1.0.0 /mplex/6.7.0 /ipfs/id/1.0.0 /ipfs/ping/1.0.0 /libp2p/relay/circuit/0.1.0 /ipfs/diag/net/1.0.0 /ipfs/kad/1.0.0 /ipfs/bitswap/1.0.0"

# Update each log

# Bandwidth (all)
echo -e $TIMESTAMP >> $FILE_BW
ipfs stats bw >> $FILE_BW
echo -e $EOR >> $FILE_BW

# Bandwidth (by proto)
for proto in $IPFS_PROTOS; do
	name=$(echo -e $proto | sed 's/\//_/g')
	proto_fn="bandwidth.$name.log"
	echo -e $TIMESTAMP >> $proto_fn
	ipfs stats bw -t "$proto" >> $proto_fn
	echo -e $EOR >> $proto_fn
done

# Bitswap
echo -e $TIMESTAMP >> $FILE_BIT
ipfs stats bitswap >> $FILE_BIT
echo -e $EOR >> $FILE_BIT

# Peers
echo -e $TIMESTAMP >> $FILE_PEERS
ipfs swarm peers >> $FILE_PEERS
echo -e $EOR >> $FILE_PEERS

# Known addresses
echo -e $TIMESTAMP >> $FILE_KNOWN
ipfs swarm addrs >> $FILE_KNOWN
echo -e $EOR >> $FILE_KNOWN

echo -e "Done with $TIMESTAMP"