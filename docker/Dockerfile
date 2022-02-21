FROM python:3.9-buster

LABEL maintainer="stevenbetancurt@hotmail.com"

WORKDIR /indicator

ENV WORKDIR=/indicator
ENV DEPENDENCIES=${WORKDIR}/dependencies
ENV DATA=${WORKDIR}/data
ENV DATA=${WORKDIR}/data

COPY ./dependencies ${DEPENDENCIES}

RUN apt-get update \
    && apt-get install -y \
        vim \
        unzip \
        git \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install -r ${DEPENDENCIES}/requirements.txt

# docker build -t stevensotelo/fao_sow:latest .
# docker tag xx stevensotelo/fao_sow:latest

# docker run -it --rm -v D:/CIAT/Code/CWR/fao_sow/src:/scripts stevensotelo/fao_sow:latest /bin/bash
