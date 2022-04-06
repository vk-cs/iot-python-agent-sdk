docker-run-mosquitto: docker-remove-mosquitto
	docker run -d \
		-p 1883:1883 \
		--name mosquitto \
		eclipse-mosquitto \
		mosquitto -c /mosquitto-no-auth.conf

docker-remove-mosquitto:
	docker rm -f mosquitto || true

tests-dep:
	python3 -m venv venv && \
		. venv/bin/activate && \
		pip install -r requirements.txt && \
		pip install -r requirements_test.txt && \
    	deactivate

tests-run:
	. venv/bin/activate && \
		pytest -W ignore -vv && \
		deactivate
