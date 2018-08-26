FROM python:3

RUN mkdir /app

WORKDIR /app

ADD requirements.txt plotly_html_template.jinja install_benchmark_database_objects.sql run_benchmarks.py ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python", "run_benchmarks.py" ]
