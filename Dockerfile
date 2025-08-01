# Use official Python 3.11 slim image
FROM python:3.11-slim

# Install Java 17 and required system packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    gcc \
    curl \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set the working directory
WORKDIR /usr/src/app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the entire app source
COPY . .

# Expose the port your app runs on
EXPOSE 8007

# Default command (also defined in docker-compose)
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "-b", "0.0.0.0:8007", "-w", "4", "--timeout", "120"]
