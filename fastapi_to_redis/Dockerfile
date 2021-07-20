FROM docker.mci.dev/docker.io/tiangolo/uvicorn-gunicorn-fastapi:python3.7
RUN pip install requests
RUN pip install pyvmomi
RUN pip install aiofiles
RUN pip install redis
COPY ./app /app
