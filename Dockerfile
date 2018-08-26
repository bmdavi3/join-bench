FROM python:3

ARG UNAME=testuser
ARG UID
ARG GID
RUN groupadd -g $GID $UNAME
RUN useradd -m -u $UID -g $GID -s /bin/bash $UNAME

COPY Pipfile Pipfile.lock plotly_html_template.jinja install_benchmark_database_objects.sql run_benchmarks.py /app/

WORKDIR /app

RUN pip install pipenv

ENV PIP_NO_BINARY=psycopg2

RUN pipenv install --system --deploy

USER $UNAME

ENTRYPOINT [ "python", "run_benchmarks.py" ]
