.PHONY: dev docker-up docker-down demo test lint clean reset-demo full-up full-down test-integration

dev:
	uvicorn airflow_copilot.main:app --host 0.0.0.0 --port 8000 --reload & \
	streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

full-up:
	cd airflow-self-hosted && docker compose up -d
	docker compose up --build -d

full-down:
	docker compose down
	cd airflow-self-hosted && docker compose down

test-integration:
	python -m pytest tests/test_integration_live.py -v --tb=short

demo:
	DEMO_MODE=true uvicorn airflow_copilot.main:app --host 0.0.0.0 --port 8000 --reload & \
	sleep 2 && \
	DEMO_MODE=true streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0

test:
	python -m pytest tests/ -v --cov=src/airflow_copilot --cov-report=term

lint:
	ruff check src/ tests/ dashboard/

clean:
	rm -f data/dag_doctor.db

reset-demo:
	curl -s -X POST http://localhost:8000/demo/reset
