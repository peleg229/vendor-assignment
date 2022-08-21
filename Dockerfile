FROM python:3.9.13-slim-bullseye

ARG USER=home365
ARG WORKDIR=/app/

WORKDIR $WORKDIR


RUN groupadd -f -g 1000 $USER && useradd -m $USER --gid 1000 && chown -R $USER:$USER $WORKDIR


RUN apt-get update
RUN apt-get install -y curl

COPY --chown=$USER:$USER ./requirements.txt ./requirements.base.txt $WORKDIR

RUN pip install --no-cache-dir --upgrade -r $WORKDIR/requirements.base.txt
RUN pip install --no-cache-dir --upgrade -r $WORKDIR/requirements.txt

COPY --chown=$USER:$USER src/ $WORKDIR/src/

RUN apt-get clean && apt-get autoclean
RUN echo 'export $(strings /proc/1/environ | grep AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)' >> /root/.profile

CMD ["uvicorn", "src.vendor-assignment.main:app", "--host", "0.0.0.0", "--port", "80"]

