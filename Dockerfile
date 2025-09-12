FROM python:alpine3.21

WORKDIR /opt/pipe-info

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY start.sh .  

RUN chmod +x start.sh

COPY src/ ./src/

ENTRYPOINT ["./start.sh"]

