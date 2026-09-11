.PHONY: up down logs check test smoke recovery
up:
	docker compose up --build -d --wait --wait-timeout 180
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
check:
	uv sync --extra dev --frozen
	uv run ruff format --check .
	uv run ruff check .
test:
	docker compose --profile test build test
	docker compose --profile test run --rm test
smoke:
	docker compose exec -T api python scripts/smoke.py
recovery:
	uv run python scripts/recovery_smoke.py
