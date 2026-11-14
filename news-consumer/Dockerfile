FROM python:3.12-slim

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy the entire project since consumer depends on root configs and schemas
COPY . .

WORKDIR /app/news-consumer

# Sync dependencies using uv
RUN uv sync --frozen

# Run the consumer using uv
CMD ["uv", "run", "main.py"]