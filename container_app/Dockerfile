FROM python:3.10-slim

WORKDIR /app

# Install system dependencies required for OpenCV and other libraries
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY .env .
COPY yolov8s.pt .
COPY tracker.py .
COPY vehicle_detection_and_speed_estimation.py .

CMD ["python", "vehicle_detection_and_speed_estimation.py"]
