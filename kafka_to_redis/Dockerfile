FROM docker.mci.dev/python:3.7
RUN  pip install kafka-python==2.0.1
RUN pip install redis

COPY ./ /
WORKDIR /

CMD ["python3","-u","main.py"]
