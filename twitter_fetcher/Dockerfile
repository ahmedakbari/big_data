FROM python:3.7
RUN  pip install kafka-python==2.0.1
RUN pip install emoji==0.5.3
RUN pip install nltk==3.4.5
RUN pip install tweepy==3.10.0
RUN pip install numpy==1.19.3
COPY ./ /
WORKDIR /

CMD ["python3","-u","main.py"]