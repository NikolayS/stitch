FROM alpine:3.21

RUN apk add --no-cache postgresql-client

COPY dist/sqlever-linux-amd64 /usr/local/bin/sqlever

ENTRYPOINT ["sqlever"]
