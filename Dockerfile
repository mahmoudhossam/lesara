FROM python:3.6

COPY . /app

RUN pip install -r /app/requirements.txt

EXPOSE 8888

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root", "/app/Lesara.ipynb"]
