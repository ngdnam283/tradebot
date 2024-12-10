#!/bin/bash
set -e  # Exit immediately if any command fails

# Define the Python script's filename
SCRIPT_NAME="main.py"

echo "Step 1: Updating package lists"
sudo apt-get update

echo "Step 2: Installing Python3 and pip"
sudo apt-get install -y python3 python3-pip python3-venv

echo "Step 3: Setting up virtual environment"
python3 -m venv .venv

echo "Step 4: Activating virtual environment"
source .venv/bin/activate

echo "Step 5: Installing dependencies (if requirements.txt exists)"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "No requirements.txt found, skipping dependencies installation."
fi

echo "Step 6: Running the Python script"
nohup python3 "$SCRIPT_NAME" &

echo "Setup complete!"
