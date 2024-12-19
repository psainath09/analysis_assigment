


# Use an official image with Java pre-installed
FROM openjdk:11-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Install required dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create a virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy the project files into the container
COPY . /app

# Create output directory
RUN mkdir -p /app/output

# Set environment variables for Spark
ENV SPARK_HOME=/opt/venv/lib/python3.9/site-packages/pyspark
ENV PATH="$SPARK_HOME/bin:$PATH"

#RUN pytest src/tests/

# Set the default command
ENTRYPOINT ["python3", "src/main/data_analysis.py"]