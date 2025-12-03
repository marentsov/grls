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
