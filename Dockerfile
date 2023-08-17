FROM python:3.11.3

WORKDIR /app

COPY src/ ./src
COPY static/ ./static/

COPY main.py test.py requirements.txt .env ./

RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt
