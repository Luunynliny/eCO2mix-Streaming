FROM python:3.12-slim-bullseye

WORKDIR /opt/python/scripts
COPY . /opt/python/scripts

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python" ,"historic_data_retrieving.py"]