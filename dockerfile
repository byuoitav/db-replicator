FROM gcr.io/distroless/static
LABEL maintainer="Clinton Reeder <clinton_reeder@byu.edu>"

ARG NAME

COPY ${NAME} /app

ENTRYPOINT ["/app"]