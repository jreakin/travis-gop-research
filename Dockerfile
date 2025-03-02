# Use Python 3.12 slim as the base image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Install curl for downloading uv
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv and update PATH in the same layer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    export PATH="/root/.local/bin:$PATH" && \
    uv --version

# Ensure PATH persists for subsequent layers
ENV PATH="/root/.local/bin:${PATH}"

# Copy dependency files first (e.g., pyproject.toml, requirements.txt if present)
COPY pyproject.toml requirements.txt* ./

# Install dependencies using uv
RUN uv pip install --system .

# Copy the rest of the application code
COPY . .

# Set the entry point for the container
ENTRYPOINT ["uv", "run", "python", "research_feb2025.py"]