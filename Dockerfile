FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5200
CMD ["gunicorn","-b","0.0.0.0:5200","app:app"]
