FROM python:3.11-slim

WORKDIR /app

# Coolify'ın sağlık kontrolü yapabilmesi için curl paketini kuruyoruz
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

EXPOSE 8000

# Command to use by default in Coolify or Docker
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
