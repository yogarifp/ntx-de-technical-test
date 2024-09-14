# Gunakan `python:3.11.4` sebagai base image
# Menggunakan image dasar Python 3.11.4
FROM python:3.11.4

# Menentukan direktori di dalam container
WORKDIR /app

# Copy file requirements.txt ke dalam container
COPY requirements.txt .

# Menginstal dependensi yang ada di requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy semua file dari dir lokal ke dalam container
COPY . .

# Ekspos port 6000 untuk API
EXPOSE 6000

# Running aplikasi ketika container dimulai
CMD ["python", "api.py"]
