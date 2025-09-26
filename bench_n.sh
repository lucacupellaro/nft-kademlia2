#!/usr/bin/env bash
set -euo pipefail

# Usage: ./bench_n.sh [START_N] [END_N]
START_N="${1:-13}"
END_N="${2:-20}"

ENV_FILE=".env"
RESULTS_CSV="results.csv"

# Script che rigenera docker-compose.yml a partire da .env
GEN_SCRIPT="./generate-compose.sh"   # <-- aggiorna il path se diverso

# >>>>>>>>>>>> CONFIG: percorsi dei volumi <<<<<<<<<<<<
DATA_DIR="/home/luca/SDCC/kademlia-nft/data"   # <-- come richiesto

# --- Helpers ---------------------------------------------------------------

backup_env() {
  cp -f "$ENV_FILE" "$ENV_FILE.bak.$(date +%s)"
}

set_env_var() {
  local key="$1" val="$2"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i.bak "s/^${key}=.*/${key}=${val}/" "$ENV_FILE"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}

get_env_var() {
  local key="$1"
  grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true
}

ensure_nodes_dirs() {
  local n="$1"
  mkdir -p "$DATA_DIR"
  for i in $(seq 1 "$n"); do
    mkdir -p "${DATA_DIR}/node$i"
  done
}

# usa sudo se necessario
maybe_sudo() {
  if [ -w "$DATA_DIR" ]; then
    "$@"
  else
    sudo "$@"
  fi
}

# pulisce SOLO i contenuti dentro /home/luca/SDCC/kademlia-nft/data/node*
clean_nodes_data() {
  echo "🧹 Pulizia dati in ${DATA_DIR}/node*/"
  # guardrail: assicurati di NON cancellare fuori dalla dir attesa
  case "$DATA_DIR" in
    /home/luca/SDCC/kademlia-nft/data) ;;
    *) echo "❌ DATA_DIR inatteso: $DATA_DIR (aborting per sicurezza)"; exit 1;;
  esac

  if [ -d "$DATA_DIR" ]; then
    for d in "$DATA_DIR"/node*; do
      [ -d "$d" ] || continue
      # elimina tutto il contenuto (ma NON la cartella nodeX stessa)
      maybe_sudo find "$d" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    done
  fi
}

dc_down() { docker compose down --remove-orphans || true; }
dc_up_build() { docker compose up --build -d; }

# Rigenera docker-compose.yml
generate_compose() {
  if [ ! -x "$GEN_SCRIPT" ]; then
    echo "❌ Script generatore non eseguibile o mancante: $GEN_SCRIPT"
    exit 1
  fi
  echo "🧩 Rigenero docker-compose.yml con $GEN_SCRIPT…"
  "$GEN_SCRIPT"
}

# Verifica che il numero di servizi node* corrisponda a N
check_compose_matches_N() {
  local n="$1"
  local count
  count=$(docker compose config --services | grep -E '^node[0-9]+$' | wc -l | tr -d ' ')
  echo "🔎 compose ha $count servizi node*, attesi: $n"
  if [ "$count" -ne "$n" ]; then
    echo "❌ Mismatch: docker-compose.yml non riflette N=$n. Controlla $GEN_SCRIPT e .env"
    exit 1
  fi
}

# Conta i file NFT (json) per ogni nodo 1..N
collect_counts() {
  local n="$1"
  local counts=()
  local total=0
  for i in $(seq 1 "$n"); do
    local d="${DATA_DIR}/node$i"
    local c=0
    if [ -d "$d" ]; then
      c=$(find "$d" -maxdepth 1 -type f -name '*.json' | wc -l | tr -d ' ')
    fi
    counts+=("$c")
    total=$(( total + c ))
  done
  echo "${counts[*]}|$total"
}

# Rimuove il primo campo (node1 / seeder) dalla stringa di conteggi
exclude_node1_counts() {
  local s="$1"
  # shellcheck disable=SC2086
  set -- $s
  if [ "$#" -ge 2 ]; then
    shift
    echo "$*"
  else
    echo ""
  fi
}

wait_stable_distribution() {
  local n="$1"
  local last_total=-1
  local stable_count=0
  local tries=0
  local max_tries=60   # ~3 minuti

  echo "⏳ Attendo stabilizzazione della distribuzione NFT…"
  while (( tries < max_tries )); do
    res=$(collect_counts "$n")
    counts_str="${res%%|*}"
    total="${res##*|}"

    if [[ "$total" == "$last_total" && "$total" -gt 0 ]]; then
      stable_count=$((stable_count + 1))
    else
      stable_count=0
    fi

    echo "   Totale NFT = $total (stable=$stable_count)  [${counts_str}]"
    if (( stable_count >= 3 )); then break; fi

    last_total="$total"
    tries=$((tries + 1))
    sleep 3
  done
}

# Calcola media e deviazione standard (popolazionali)
compute_mean_std() {
  LC_ALL=C awk '
    {
      n=NF; sum=0
      for (i=1; i<=NF; i++) { x[i]=$i; sum+=x[i] }
      mean = (n>0)? sum/n : 0
      s2=0
      for (i=1; i<=NF; i++) { d=x[i]-mean; s2+=d*d }
      var = (n>1)? s2/n : 0        # usa s2/(n-1) se vuoi la campionaria
      std = (var>0)? sqrt(var) : 0
      printf("%.6f %.6f\n", mean, std)
    }' <<< "$*"
}

# ----- CSV: sovrascrivi sempre ------------------------------------------------
init_results_csv() {
  printf "N,avg,std\n" > "$RESULTS_CSV"
  echo "📄 CSV inizializzato (overwrite): ${RESULTS_CSV}"
}

append_csv() {
  local n="$1" mean="$2" std="$3"
  echo "${n},${mean},${std}" >> "$RESULTS_CSV"
  echo "📝 Write: N=${n}, avg=${mean}, std=${std} -> ${RESULTS_CSV}"
}

# --- Main ------------------------------------------------------------------

if [ ! -f "$ENV_FILE" ]; then
  echo "Errore: non trovo ${ENV_FILE} nella dir corrente."
  exit 1
fi

# CSV nuovo a ogni esecuzione
init_results_csv

backup_env
trap 'echo "Nota: backup creati ${ENV_FILE}.bak*";' EXIT

BUCKET_SIZE="$(get_env_var BUCKET_SIZE || echo 5)"
REPLICATION_FACTOR="$(get_env_var REPLICATION_FACTOR || echo 3)"
echo "USO: DATA_DIR=${DATA_DIR}, BUCKET_SIZE=${BUCKET_SIZE}, REPLICATION_FACTOR=${REPLICATION_FACTOR}"

for N in $(seq "$START_N" "$END_N"); do
  echo "=========================================================="
  echo "🔧 Run per N=${N}"
  echo "=========================================================="

  set_env_var N "$N"
  set_env_var BUCKET_SIZE "$BUCKET_SIZE"
  set_env_var REPLICATION_FACTOR "$REPLICATION_FACTOR"

  # (1) Rigenera docker-compose.yml e verifica che rispetti N
  generate_compose
  check_compose_matches_N "$N"

  # (2) Prepara dirs, down, pulizia, up
  ensure_nodes_dirs "$N"
  dc_down
  clean_nodes_data
  dc_up_build

  # (3) Attendi stabilizzazione e misura
  wait_stable_distribution "$N"

  res=$(collect_counts "$N")
  counts="${res%%|*}"

  # Escludi node1 (seeder) dalle statistiche
  counts_no1="$(exclude_node1_counts "$counts")"

  # Calcola media e std sui nodi 2..N
  read -r mean std < <(compute_mean_std "$counts_no1")
  append_csv "$N" "$mean" "$std"

  # (4) Down per run successivo pulito
  dc_down
done

echo "✅ Finito. Vedi ${RESULTS_CSV}"
