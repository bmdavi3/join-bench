FROM python:3

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY plotly_html_template.jinja install_benchmark_database_objects.sql run_benchmarks.py ./

CMD [ "python", "./run_benchmarks.py", "/input/input.json", "--output-dir", "/output" ]
