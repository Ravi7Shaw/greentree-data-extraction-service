#!/usr/bin/env bash
set -e

echo "== HubSpot Deals ETL =="

if ! command -v python >/dev/null 2>&1; then
    echo "Python is required."
    echo "Arch Linux: sudo pacman -S python"
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required."
    echo "Arch Linux: sudo pacman -S docker docker-compose"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running."
    echo "Start it with: sudo systemctl start docker"
    exit 1
fi

if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    python -m venv .venv
fi

source .venv/bin/activate

echo "Installing Python dependencies..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if [ ! -f ".env" ]; then
    echo
    echo "Missing .env file."
    echo "Run:"
    echo "  cp .env.example .env"
    echo "Then edit .env with your configuration."
    exit 1
fi

echo "Starting PostgreSQL and Redis..."
docker compose up -d postgres redis

echo "Starting Flask API..."
exec python app.py
