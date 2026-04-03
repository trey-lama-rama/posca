#!/bin/bash
# setup.sh — Initialize Headless CRM
# Creates database, applies schema, copies config templates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Headless CRM Setup ==="

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt -q

# Copy config templates if they don't exist
if [ ! -f config.yaml ]; then
    cp config.yaml.example config.yaml
    echo "Created config.yaml — edit this with your account details."
else
    echo "config.yaml already exists, skipping."
fi

if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env — add your API keys and passwords."
else
    echo ".env already exists, skipping."
fi

# Create directories
mkdir -p data logs

# Initialize database
echo "Initializing database..."
sqlite3 data/crm.db < schema.sql
python3 migrations/migrate_personal_fields.py

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit config.yaml with your email accounts and settings"
echo "  2. Edit .env with your API keys and passwords"
echo "  3. Run: bash sync.sh"
echo ""
echo "For nightly automation, add to crontab:"
echo "  0 2 * * * cd $(pwd) && bash sync.sh >> logs/cron.log 2>&1"
