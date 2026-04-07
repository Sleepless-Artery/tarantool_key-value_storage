# Key-Value Store gRPC Service

**gRPC-сервис для хранения пар 'ключ-значение' с поддержкой `null` на базе Tarantool 3.2.x**. 


## 📋 Методы API

| Метод | Описание | Параметры | Возвращаемое значение |
|-------|----------|-----------|----------------------|
| `Put` | Сохраняет новую пару или перезаписывает значение для существующего ключа | `key: string`, `value: optional bytes` | — |
| `Get` | Возвращает значение для указанного ключа | `key: string` | `value: optional bytes` (может отсутствовать → `null`) |
| `Delete` | Удаляет запись по ключу | `key: string` | — |
| `Range` | Возвращает **gRPC stream** пар ключ-значение в заданном диапазоне | `key_since: string`, `key_to: string` | `stream {key: string, value: optional bytes}` |
| `Count` | Возвращает общее количество записей в БД | — | `count: int64` |

---
## 🚀 Быстрый старт

### 1. Требования
```bash
# Java 21+
java -version

# Maven 3.8+
mvn -version

# Docker & Docker Compose
docker --version
docker compose version
```

### 2. Запуск
```bash
# Клонируйте репозиторий
git clone https://github.com/Sleepless-Artery/tarantool_key-value_storage.git

# Перейдите в директорию репозитория
cd tarantool_key-value_storage

# Запустите Tarantool
docker compose up -d

# Соберите проект
mvn clean compile

# Запустите сервис
mvn exec:java
```


### 3. Тестирование API

**Через `grpcurl`**

#### Put
```bash
# Задаем значение
grpcurl -plaintext -d "{\"key\":\"a\",\"value\":\"MQ==\"}" localhost:9090 KVService/Put

# Не указываем значение, value становится null
grpcurl -plaintext -d "{\"key\":\"b\"}" localhost:9090 KVService/Put
```

#### Get
```bash
grpcurl -plaintext -d "{\"key\":\"a\"}" localhost:9090 KVService/Get
```

#### Range
```bash
# Получаем все записи (ключ-значение) в диапазоне от "a" до "z" включительно
grpcurl -plaintext -d "{\"key_since\":\"a\",\"key_to\":\"z\"}" localhost:9090 KVService/Range
```

#### Count
```bash
grpcurl -plaintext -d "{}" localhost:9090 KVService/Count
```

#### Delete
```bash
grpcurl -plaintext -d "{\"key\":\"a\"}" localhost:9090 KVService/Delete
```


### 4. Остановка

В терминале с `mvn exec:java` остановите выполнение программы при помощи `Ctrl+C` и выполните:
```bash
docker compose down
```