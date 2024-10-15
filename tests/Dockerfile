# Use the official Python base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# install curl
RUN apt-get update && apt-get install -y curl

# Copy the requirements file to the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Set the command to run the application
CMD ["bash", "integration_test.sh"]