build:
	docker build -t angryjoin .

run: build
	docker run -v /home/brian/angryjoin:/input -v /home/brian/angryjoin/benchmark_results:/output --network host --env PGPASSWORD --env PGHOST --env PGDATABASE --env PGUSER -it --rm --name my-running-angryjoin angryjoin
