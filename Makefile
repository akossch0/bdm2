SHELL := /bin/bash

.PHONY: help env install format formatted-pipeline exploitation-multidim model-training model-inference

env:
	@echo "Setting src folder to PYTHONPATH..."
	@echo "Creating virtual environment..."
	if [ ! -d ".venv" ]; then python -m venv .venv; fi
	python -m venv .venv

# Install dependencies
install:
	@echo "Activating virtual environment..."
	source .venv/bin/activate
	@echo "Installing dependencies..."
	pip install -r requirements.txt

# Code formatting using black and isort
format:
	@echo "Running code formatting..."
	source .venv/bin/activate
	black .
	isort .

# Run pipeline
formatted-pipeline:
	@echo "Running formatting pipeline..."
	source .venv/bin/activate
	python -m src.formatted_pipeline

# Run pipeline for multidimensional exploitation zone
exploitation-multidim:
	@echo "Running pipeline for multidimensional exploitation zone..."
	source .venv/bin/activate
	python -m src.exploitation.multidimensional

# Run pipeline for model training
model-training:
	@echo "Running pipeline for model training..."
	source .venv/bin/activate
	python -m src.exploitation.train

# Run pipeline for model inference
model-inference:
	@echo "Running pipeline for model inference..."
	source .venv/bin/activate
	python -m src.exploitation.predict

# Help target to display available targets and their descriptions
help:
	@echo "Available targets:"
	@echo "  env: Create virtual environment"
	@echo "  install: Install dependencies"
	@echo "  format: Run code formatting using black"
	@echo "  formatted-pipeline: Run pipeline for formatted zone"
	@echo "  exploitation-multidim: Run pipeline for multidimensional exploitation zone"
	@echo "  help: Display available targets and their descriptions"
