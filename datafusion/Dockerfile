FROM bitnami/spark:latest

# Switch to root for installations
USER root

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Update package list and install Python pip
RUN apt-get update && apt-get install -y python3-pip python3-dev

# Upgrade pip and install requirements
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["python3", "-m", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
