# ---- Stage build ----
FROM golang:1.23-alpine AS build

WORKDIR /app

# 1) Cache mod
COPY go.mod go.sum* ./
RUN go mod download || true

# 2) Codice (copre anche ./csv)
COPY . .

# 3) (opzionale) forza modules
ENV GO111MODULE=on

# 4) Build dell'intero package cmd
RUN go build -o /node ./cmd/container

# ---- Stage runtime ----
FROM alpine:3.20
WORKDIR /app

# Binario
COPY --from=build /node /app/node

# CSV dentro l'immagine finale
COPY --from=build /app/csv /app/csv

CMD ["/app/node"]
