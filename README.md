# Big Data Management - Project 2

This is the repository for the second project of the Big Data Management course. It consists of the implementation of the Formatted and Exploitation zones in the Data Management Backbone.

## Project setup
**Pre-requisites**:
- `python >= 3.8`
- `pip >= 21.0`
- `make >= 4.2.1`

**Create venv, and install dependencies**:
```bash
make install
```

**Run setup script that sets necessary environment variables**:
```bash
source setup.sh
```

**Create .env file for pipeline configuration**:
Create a `.env` file based on the `.env_template` file and fill in the necessary information (depending on the MongoDB, PostgreSQL, and MLflow deployments, the hosts 
and ports for said tools need to be adjusted). The other variables are already set to the default values. The MongoDB collection on which the predictive exploitation zone needs to be adjusted depending on predictive needs, as well as the model, transformation pipeline name and versions.

**Run the formatted pipeline**:
```bash
make formatted-pipeline 
```

**Run the multidimensional exploitation pipeline**:
```bash
make exploitation-multidim
```

**Run model training pipeline**:
```bash
make model-training
```

**Run model inference pipeline**, adjust target collection and model/transformation pipeline version in the `.env` file:
```bash
make model-inference
```

## OpenNebula credentials
- user: masterBD20
- password: D4qKbYH3XR

# Appendix
We had some problems with the OpenNebula platform, so we decided to also use a local MongoDB and PostgreSQL instances with Docker. Below are the instructions to set up the instances.
## Setup PostgreSQL instance with docker

Prerequisites:
- `docker`

1. Pull the PostgreSQL image:
```bash
docker pull postgres
```

2. Run the PostgreSQL container:
```bash
docker run -itd -e POSTGRES_USER=<USER> -e POSTGRES_PASSWORD=<PASSWORD> -p 5432:5432
```

## Setup MongoDB instance with docker

We also made a local installation of MongoDB with Docker. [Here](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-community-with-docker/) is the link to the setup instructions.

## Additional dataset - Air quality
As part of the project, we decided to integrate air quality data from the Open Data BCN platform. The data is available in two datasets:
- stations - https://opendata-ajuntament.barcelona.cat/data/ca/dataset/qualitat-aire-estacions-bcn
- measurements - https://opendata-ajuntament.barcelona.cat/data/en/dataset/qualitat-aire-detall-bcn


# Contributors
- Darryl Abraham
- Ákos Schneider
