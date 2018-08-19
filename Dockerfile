FROM python:3

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY run_benchmarks.py .
COPY install_benchmark_database_objects.sql .
COPY plotly_html_template.jinja .

CMD [ "python", "./run_benchmarks.py", "/input/input.json" ]
