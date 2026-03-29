FROM python:3.11-slim

LABEL \
  io.hass.name="Gold Intelligence" \
  io.hass.description="Gold market analysis with Claude AI + Smart Money Score" \
  io.hass.type="addon" \
  io.hass.version="1.0.0"

WORKDIR /app

# Dipendenze di sistema
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App
COPY gold_engine.py .
COPY main.py .
COPY index.html .
COPY run.sh /run.sh

RUN chmod a+x /run.sh

CMD ["/run.sh"]
