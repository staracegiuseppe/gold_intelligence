FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gold_engine.py .
COPY market_data.py .
COPY signal_engine.py .
COPY mailer.py .
COPY main.py .
COPY index.html .
COPY run.sh /run.sh
RUN chmod a+x /run.sh
CMD ["/run.sh"]
