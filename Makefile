build:
	docker build --tag take-home .
run:
	docker-compose up run
test:
	docker-compose up test
dev:
	docker exec -it data-enginering-take-home-main_run_1 /bin/bash
shutdown:
	docker stop $$(docker ps -aq) && docker rm $$(docker ps -aq)
