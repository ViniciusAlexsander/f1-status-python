FROM python:3.12-slim

WORKDIR /app

# 1. Copia o requirements da raiz para o container
COPY requirements.txt .

# 2. Agora o pip vai encontrar o arquivo e instalar o uvicorn
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copia todo o projeto (incluindo a pasta api)
COPY . /app

ENV PYTHONPATH=/app

# 4. Como a pasta 'api' foi copiada, o caminho 'api.main:app' funcionará perfeitamente
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]