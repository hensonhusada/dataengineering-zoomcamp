FROM python:slim

RUN apt-get update && apt-get install -y wget

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]