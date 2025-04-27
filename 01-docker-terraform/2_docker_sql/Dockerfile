# Install image with a Linux OS on it and python installed and ready 
FROM python:3.9.1

# Update packages and install wget
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

# sets the working directory of the container to /app, so every command following will be executed here
WORKDIR /app

# copies local ingest to app/ingest
COPY ingest_data.py ingest_data.py 

# when image is run will run python ingest_data.py 
ENTRYPOINT [ "python", "ingest_data.py" ]