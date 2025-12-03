# Медицинский парсер ГРЛС 

Система для автоматического сбора, анализа и версионирования данных из Государственного реестра лекарственных средств (ГРЛС).

### Поток данных
1. **Загрузка данных**: Celery задача запускается по расписанию (9:00, 18:00)
2. **Парсинг страницы**: Система заходит на главную страницу ГРЛС, находит ссылку на свежий архив
3. **Скачивание и обработка**: Архив скачивается, распаковывается, находится файл "Действующий"
4. **Анализ Excel**: Файл анализируется, находятся связи между активными веществами и препаратами
5. **Версионирование**: Данные сохраняются в БД, изменения отслеживаются
6. **Очистка**: Старые файлы удаляются (раз в месяц), данные в БД сохраняются

### Ключевые особенности
- **Автоматическое обновление**: Система сама находит свежие данные
- **Полная история**: Все изменения препаратов сохраняются
- **Отказоустойчивость**: Ошибки в обработке одного препарата не влияют на остальные
- **Масштабируемость**: Каждый препарат обрабатывается в отдельной транзакции

## Структура БД

Этот проект использует PostgreSQL для хранения данных из ГРЛС. База данных инициализируется скриптом `app/scripts/init-database.sql`. Поддерживается версионирование: старые версии данных помечаются как `is_current = FALSE`

### Таблицы

| Таблица | Описание | Ключевые поля |
|---------|----------|---------------|
| **analysis_sessions** | Сессии анализа (каждый прогон пайплайна). | `id` (PK), `timestamp`, `source_file`, `total_records`, `substances_found`, `preparations_found`, `consumers_found` |
| **substance_manufacturers** | Производители субстанций с версионированием. | `id` (PK), `substance_name`, `manufacturers` (JSONB), `first_seen_date`, `last_seen_date`, `is_current`, `version` |
| **substance_manufacturer_changes** | Журнал изменений производителей субстанций. | `id` (PK), `substance_name`, `old_manufacturers` (JSONB), `new_manufacturers` (JSONB), `change_type` ('added'/'modified'), `session_id` (FK) |
| **substance_consumers** | Препараты (потребители субстанций) с версионированием. Уникальность по комбинации полей. | `id` (PK), `substance_name`, `preparation_trade_name`, `preparation_inn_name`, `preparation_manufacturer`, `preparation_country`, `registration_number`, `registration_date`, `release_forms`, `is_current`, `version` |
| **substance_consumer_changes** | Журнал изменений препаратов. | `id` (PK), `substance_name`, `preparation_trade_name`, `preparation_inn_name`, `preparation_manufacturer`, `preparation_country`, `registration_number`, `change_type` ('added'/'modified'), `changed_fields` (JSONB), `session_id` (FK) |

- **Версионирование**: При изменениях создается новая запись с инкрементной `version`, старая помечается `is_current = FALSE`.
- **Очистка**: Задача `cleanup_old_files_task` удаляет файлы старше 30 дней (настраивается).


### Примеры аналитических запросов в файле queries.txt

## Запуск

Для запуска сервисов используйте:

```
docker-compose up --build
```

Для мониторинга - Celery через Flower: http://localhost:5555

