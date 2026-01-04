FROM ubuntu:latest
LABEL authors="rodrigo"

ENTRYPOINT ["top", "-b"]