#!/bin/bash 

echo "This script will run the app in local development mode."
echo "*****************************************************"

# activate virtual environment
echo "Activating [orbis-venv] virtual environment..."
source ~/Documents/Projects/orbis-venv/bin/activate
echo "[orbis-venv] virtual environment activated."

# check whether virtual environment is activated
if [ "$VIRTUAL_ENV" = "" ]; then
    echo "*****************************************************"
    echo "ERROR: [orbis-venv] virtual environment not activated. Please activate the virtual environment and try again."
    echo "*****************************************************"
    exit 1
fi

echo "*****************************************************"
# check if config/config.yaml file exists 
if [ ! -f ./config/config.yaml ]; then
    echo "Config file not found. Creating config file..."
    cp ./config/config-sample.yaml ./config/config.yaml
    echo "Config file created."
    echo "*****************************************************"
    echo "PLEASE UPDATE THE CONFIG FILE WITH YOUR OWN VALUES. BEFORE CONTINUING. !!!"
    echo "*****************************************************"
fi

# run the app
LOCAL_DEV=True CONFIG_PATH=./config/config.yaml python orbi/orbi.py
