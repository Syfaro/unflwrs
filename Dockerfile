FROM ubuntu:22.04
EXPOSE 8080
WORKDIR /app
COPY ./static ./static
COPY ./unflwrs /bin/unflwrs
CMD ["/bin/unflwrs"]
