.PHONY: run stop build test shell

IMAGE = ustas007/pipelines-the-right-way:chapter-01

run:
	bash docker/scripts/start.sh

stop:
	bash docker/scripts/stop.sh

build:
	docker compose -f docker/docker-compose.yaml build

test:
	docker build -t $(IMAGE) -f docker/Dockerfile .
	docker run --rm -e PYTHONPATH=/app/src -e PTWR_DATA_DIR=/app/data -v $(PWD)/data:/app/data $(IMAGE) pytest -q

shell:
	docker run --rm -it -e PYTHONPATH=/app/src -e PTWR_DATA_DIR=/app/data -v $(PWD)/data:/app/data $(IMAGE) bash
