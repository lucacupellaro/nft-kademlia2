#!/usr/bin/env bash
set -euo pipefail

N=11

# NODES = "node2,node3,..."
NODES=""
for i in $(seq 2 "$N"); do
  NODES+="node$i,"
done
NODES="${NODES%,}"

# (Consiglio) prepara le cartelle per i bind mounts
mkdir -p data
for i in $(seq 1 "$N"); do
  mkdir -p "data/node$i"
done

cat > docker-compose.yml <<EOF
services:
EOF

for i in $(seq 1 "$N"); do
  cat >> docker-compose.yml <<EOF
  node$i:
    image: "kademlia-nft-node:latest"
    build: .
    environment:
      - NODE_ID=node$i
      - DATA_DIR=/data
EOF

  if [ "$i" -eq 1 ]; then
    cat >> docker-compose.yml <<EOF
      - SEED=true
      - NODES=$NODES
EOF
  fi

  cat >> docker-compose.yml <<EOF
    ports:
      - "$((8000 + i)):8000"
    volumes:
      - ./data/node$i:/data
    networks:
      - kadnet

EOF
done

cat >> docker-compose.yml <<EOF
networks:
  kadnet:
EOF
