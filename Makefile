build:
	docker build -t angryjoin .

run:
	docker run -it --rm --name my-running-angryjoin angryjoin
