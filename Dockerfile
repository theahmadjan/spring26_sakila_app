# 1. Use an appropriate slim base image as required 
FROM python:3.9-slim

# 2. Add your labels AFTER the FROM instruction 
LABEL maintainer="Ahmad <ahmad@example.com>"
LABEL version="1.0.0"
LABEL description="Sakila Flask Application - Optimized"

# 3. Set working directory [cite: 132]
WORKDIR /app

# 4. Install dependencies using requirements.txt for better caching 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 

# 5. Copy the rest of the app [cite: 133]
COPY . .

# 6. Use a non-root user for security 
RUN useradd -m sakilauser
USER sakilauser

# 7. Expose only the necessary port [cite: 141, 151]
EXPOSE 5000

# 8. Add a healthcheck 
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:5000/ || exit 1

# 9. Start the app [cite: 144]
CMD ["python", "app.py"]