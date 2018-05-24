FROM python

WORKDIR /pipeline

ADD . /pipeline

RUN pip install --trusted-host pypi.python.org -r news_pipeline/requirements.txt

CMD [ "sh", "run_forever.sh" ]