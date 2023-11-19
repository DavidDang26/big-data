FROM python:3.9-slim-buster
WORKDIR /app
COPY ./app/requirements.txt /app
RUN pip install -r requirements.txt
COPY ./app .
EXPOSE 5000
ENV FLASK_APP=main.py
CMD ["flask", "run", "--host", "0.0.0.0"]