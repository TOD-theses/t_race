FROM python:3.10-slim
RUN apt-get update && apt-get install git -y
COPY . /app
WORKDIR /app
RUN pip install .
ENTRYPOINT [ "t_race" ]
CMD [ "-h" ]
