FROM alpine:latest
RUN apk add --update --upgrade --no-cache py3-aiohttp py3-orjson py3-aiosqlite && \
    rm -rf /var/cache/apk/* && \
    fc-cache -f

ENV SOURCE_URL 0.0.0.0:5001
ENV SOURCE_PATH /pdf/json
COPY app /app
WORKDIR /app/

EXPOSE 8080

CMD python3 main.py
