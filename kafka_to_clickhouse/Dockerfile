FROM python:3.7
RUN  pip install kafka-python==2.0.1
RUN pip install clickhouse-driver
RUN apt-get update && apt-get install curl -y
COPY ./ /
WORKDIR /

CMD ["python3","-u","main.py"]
