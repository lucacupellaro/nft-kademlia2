#!/usr/bin/env bash
set -euo pipefail

# --- Carica variabili da .env (sul tuo host) ---
if [[ ! -f .env ]]; then
  echo "❌ File .env non trovato nella cartella corrente."
  echo "   Crea .env con almeno: N=11, BUCKET_SIZE=..., REPLICATION_FACTOR=..."
  exit 1
fi
set -a
# shellcheck disable=SC1091
source ./.env
set +a

# --- Validazioni minime ---
: "${N:?Manca N in .env}"
if ! [[ "$N" =~ ^[0-9]+$ ]] || [[ "$N" -lt 1 ]]; then
  echo "❌ N deve essere un intero >= 1 (N=$N)"
  exit 1
fi

# --- Costruisci lista nodi "node2,node3,..." per il seed ---
NODES=""
for i in $(seq 2 "$N"); do
  NODES+="node$i,"
done
NODES="${NODES%,}"  # rimuovi ultima virgola

# --- Prepara volumi locali ---
mkdir -p data
for i in $(seq 1 "$N"); do
  mkdir -p "data/node$i"
done

# --- Inizio compose ---
cat > docker-compose.yml <<EOF
services:
EOF

for i in $(seq 1 "$N"); do
  HOST_PORT=$((8000 + i))  # porta host incrementale
  cat >> docker-compose.yml <<EOF
  node$i:
    image: "kademlia-nft-node:latest"
    build: .
    env_file:
      - .env
    environment:
      - NODE_ID=node$i
      - DATA_DIR=/data
EOF

  # Solo il seed ha SEED=true e la lista NODES
  if [ "$i" -eq 1 ]; then
    cat >> docker-compose.yml <<EOF
      - SEED=true
      - NODES=$NODES
EOF
  fi

  cat >> docker-compose.yml <<EOF
    ports:
      - "${HOST_PORT}:8000"
    volumes:
      - ./data/node$i:/data
    networks:
      - kadnet

EOF
done

cat >> docker-compose.yml <<'EOF'
networks:
  kadnet:
EOF

echo "✅ docker-compose.yml generato."

