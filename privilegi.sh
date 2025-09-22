# vedi proprietario e permessi
ls -ld ./data
ls -ld ./data/* 2>/dev/null || true

# correggi proprietario ricorsivo alla tua utenza
sudo chown -R "$USER":"$USER" ./data

# dai permessi di scrittura a te (e al gruppo)
chmod -R u+rwX,g+rwX ./data
# opzionale: assicurati che la dir radice sia scrivibile
chmod 775 ./data
