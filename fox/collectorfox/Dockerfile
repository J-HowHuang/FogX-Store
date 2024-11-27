# Use an official Python runtime as the base image
FROM python:3.10
# Set the working directory in the container
WORKDIR /app

# Install curl
RUN apt-get -y update && apt-get install -y curl

# Copy the requirements file into the container
COPY requirements.txt .

# Install git
RUN apt-get install -y git git-lfs

# Install the Python dependencies
RUN pip install git+https://github.com/asd08573064/lerobot
RUN pip install --upgrade --no-cache-dir pip
RUN pip install --no-cache-dir -r requirements.txt


# Copy the rest of the application code into the container
COPY ./ .
RUN mkdir db
RUN mkdir -p /app/data/parquets

# Set the command to run the application
CMD [ "python", "app.py" ]