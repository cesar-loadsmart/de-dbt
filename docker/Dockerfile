FROM python:3.6-stretch 

WORKDIR /app


ARG PIP_EXTRA_INDEX_URL

COPY ./requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt --extra-index-url=$PIP_EXTRA_INDEX_URL

ADD etl/ /app/etl/

# COPY ./requirements.txt /tmp/requirements.txt
# RUN cd /tmp/ && pip3 install -r requirements.txt

# RUN pip install -r requirements.txt h
#
