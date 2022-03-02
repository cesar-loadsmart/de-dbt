FROM python:3.9-slim
WORKDIR '/app'

ARG PIP_EXTRA_INDEX_URL
RUN /usr/local/bin/python -m pip install --upgrade pip
COPY ./requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY ./de_dbt ./de_dbt