FROM python:3.8.14-slim

RUN rm -rf /var/lib/apt/lists/*

WORKDIR /home/runner/server

ENV PATH="${PATH}:/home/runner/.local/bin"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./ ./

EXPOSE 8080

CMD  uvicorn main:app --host 0.0.0.0 --port 8080
