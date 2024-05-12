# Big Data Management - Project 2

This is the repository for the second project of the Big Data Management course. It consists of the implementation of the Formatted and Exploitation zones in the Data Management Backbone.

## Additional dataset - Air quality
As part of the project, we decided to integrate air quality data from the Open Data BCN platform. The data is available in two datasets:
- stations - https://opendata-ajuntament.barcelona.cat/data/ca/dataset/qualitat-aire-estacions-bcn
- measurements - https://opendata-ajuntament.barcelona.cat/data/en/dataset/qualitat-aire-detall-bcn

## Project setup
**Pre-requisites**:
- Python >= 3.8
- Pip >= 21.0

### **Windows**
prerequisites:
- chocolatey

**Install make**:
(run as administrator)
```PowerShell
choco install make
```

**Create venv, and install dependencies**:
```PowerShell
make install
```

**Create .env file for pipeline configuration**:
Create a .env file based on the .env_template file and fill in the necessary information (only HDFS_URL needs to be adjusted, pointing to the HDFS namenode address). The other variables are already set to the default values.

**Run the pipeline**:
```PowerShell
make pipeline
```

### **UNIX-like systems**
**Create venv, and install dependencies**:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Create .env file for pipeline configuration**:
Same as in the Windows setup.

**Run the pipeline**:
```bash
source .venv/bin/activate
python src/data_io/pipeline.py
```

## OpenNebula credentials
- user: masterBD20
- password: D4qKbYH3XR

# Appendix
We had some problems with the OpenNebula platform, so we decided to also use a local Hadoop cluster with Docker. Below are the instructions to set up the cluster.
## Setup Hadoop cluster locally with docker

Prerequisites:
- Docker
- Docker Compose

1. Go to desired directory and clone the repository:
```PowerShell
git clone https://github.com/big-data-europe/docker-hadoop
```

2. Go to the cloned directory:
```PowerShell
cd docker-hadoop
```

3. Start the cluster (takes a couple of minutes):
```PowerShell
docker-compose up -d
```

4. Check if the cluster is running:
```PowerShell
docker ps
```

5. Enter the namenode container:
```PowerShell
docker exec -it namenode bash
```

6. Create a directory in HDFS:
```PowerShell
hadoop fs -mkdir -p /user/bdm
```

7. Give permissions to the directory:
```PowerShell
hadoop fs -chmod -R 777 /user/bdm
```