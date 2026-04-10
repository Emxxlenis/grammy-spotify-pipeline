FROM apache/airflow:2.9.3

# Instala las dependencias del proyecto en la imagen personalizada
# Esto evita reinstalarlas cada vez que el contenedor arranca
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
