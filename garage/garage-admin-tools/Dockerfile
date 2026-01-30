FROM alpine:3.23

RUN apk add --no-cache curl jq

COPY garage-init.sh /usr/local/bin/garage-init.sh

RUN chmod +x /usr/local/bin/garage-init.sh
