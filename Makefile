.PHONY: help up down logs worker beat flower test clean

help:
	@echo "Available commands:"
	@echo "  make up       - Start all services (Docker)"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - Show logs"
	@echo "  make worker   - Start worker manually"
	@echo "  make beat     - Start beat manually"
	@echo "  make flower   - Start flower manually"
	@echo "  make test     - Run test task"
	@echo "  make clean    - Clean up"

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

worker:
	celery -A config.celery worker --loglevel=info

beat:
	celery -A config.celery beat --loglevel=info

flower:
	celery -A config.celery flower --port=5555

test:
	celery -A config.celery call app.tasks.health_check_task

clean:
	docker-compose down -v
	rm -rf logs/*.log
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

status:
	@echo "=== Service Status ==="
	@docker-compose ps
	@echo "\n=== Redis Keys ==="
	@redis-cli -h localhost KEYS "*" | head -10

# Запуск конкретной задачи вручную
run-health:
	celery -A config.celery call app.tasks.health_check_task

run-daily:
	celery -A config.celery call app.tasks.sample_daily_task --args='["Manual daily task"]'

run-long:
	celery -A config.celery call app.tasks.long_running_task --args='[5]'