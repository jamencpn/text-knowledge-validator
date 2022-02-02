FROM tiangolo/uvicorn-gunicorn:python3.7-alpine3.8
# ENV TZ "Asia/Bangkok"
# RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apk update && apk add  --no-cache --virtual build-dependencies libc-dev build-base
COPY ./app /app
WORKDIR /app
RUN pip install -r requirements.txt
WORKDIR /app/src
# CMD uvicorn call_api:app