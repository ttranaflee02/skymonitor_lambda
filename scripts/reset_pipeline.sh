#!/bin/bash
set -e

# Stop current containers
echo "Stopping containers..."
docker compose down

# Remove volumes
echo "Removing volumes..."
docker compose down -v

# Start services again
echo "Starting services..."
docker compose up -d --build

echo "Waiting for services to initialize..."
echo "Done. Please wait a few moments for all services (Kafka, MinIO, Spark, ES) to be fully ready."
