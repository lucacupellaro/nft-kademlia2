Istruzioni per buildare la rete e popolare le risorse:

1. Configurare i parametri nel file .env
2. Generare il file docker-compose.yml:
   bash generate-compose.sh
3. Avviare la rete (build + run dei container):
   docker compose up --build
   -> a questo punto la rete è attiva e popolata con le risorse
4. Avviare la dashboard grafica:
   go run cmd/cli/main.go
5. Eseguire i test:
   - impostare la variabile TEST nel file .env
   - avviare il benchmark:
     go run cmd/bench_lookup/main.go
