# Use Python 3.10.11 as base image
FROM python:3.10.11-slim

# Set the working directory
WORKDIR /app

# Copy the application files
COPY . .

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Expose the necessary ports (Streamlit and Flask)
EXPOSE 8501 8080

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run Flask and Streamlit together
CMD ["sh", "-c", "python Streamlit_UI.py & streamlit run Streamlit_UI.py --server.port=8501 --server.address=0.0.0.0"]
