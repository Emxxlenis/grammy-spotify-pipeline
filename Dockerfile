FROM apache/airflow:2.9.3

# Install project dependencies into the image (avoids reinstalling on every container start).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
