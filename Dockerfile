FROM python:3.11-slim

WORKDIR /app

# Build deps separati dal codice — layer cacheable
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# 1. Dipendenze Python (cambiano raramente → layer stabile)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Dati statici
COPY assets.json .

# 3. Moduli Python (ordinati per frequenza di modifica)
COPY market_data.py .
COPY signal_engine.py .
COPY macro_layer.py .
COPY fundamental_layer.py .
COPY institutional_layer.py .
COPY sector_rotation_layer.py .
COPY scoring_engine.py .
COPY ai_validation.py .
COPY backtest_engine.py .
COPY smart_money.py .
COPY mailer.py .
COPY main.py .

# 4. Frontend (cambia spesso → ultimo layer)
COPY index.html .

# 5. Entrypoint
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
