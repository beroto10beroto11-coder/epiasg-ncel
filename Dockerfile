FROM python:3.11-slim

WORKDIR /app

# Coolify'ın sağlık kontrolü yapabilmesi için curl paketini kuruyoruz
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Docker seviyesinde doğrudan 3000 portunu zorlayan sağlık kontrolü
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://127.0.0.1:3000/ || exit 1

EXPOSE 3000
# Command to use by default in Coolify or Docker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
