FROM python

WORKDIR /pipeline

ADD . /pipeline

RUN pip install --trusted-host pypi.python.org -r news_pipeline/requirements.txt

ENV PYTHONUNBUFFERED=0

CMD [ "sh", "run_forever.sh" ]