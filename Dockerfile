FROM python

WORKDIR /pipeline

ADD ./news_pipeline/requirements.txt /pipeline/news_pipeline/requirements.txt

RUN pip install --trusted-host pypi.python.org -r news_pipeline/requirements.txt

ADD . /pipeline

ENV PYTHONUNBUFFERED=0

CMD [ "sh", "run_forever.sh" ]