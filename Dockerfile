FROM rust:1.76-slim-bookworm as builder

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Создание пустого проекта
RUN mkdir -p /app/src
WORKDIR /app

# Копирование Cargo.toml
COPY Cargo.toml .

# Обновление имени проекта в Cargo.toml
RUN sed -i 's/name = "t-candles"/name = "t-indicators"/' Cargo.toml
RUN sed -i 's/version = "0.1.0"/version = "0.1.0"/' Cargo.toml

# Создание пустого файла main.rs
RUN mkdir -p src && echo "fn main() {}" > src/main.rs

# Сборка зависимостей
RUN cargo build --release

# Копирование исходного кода
COPY src ./src
COPY config ./config

# Перестройка с реальным исходным кодом
RUN touch src/main.rs && cargo build --release

# Финальный образ
FROM debian:bookworm-slim

# Установка зависимостей времени выполнения
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Копирование собранного бинарного файла
COPY --from=builder /app/target/release/t-indicators /usr/local/bin/

# Копирование файлов конфигурации
COPY --from=builder /app/config /app/config

# Создание непривилегированного пользователя
RUN groupadd -r appuser && useradd -r -g appuser appuser
USER appuser

# Рабочая директория
WORKDIR /app


# Настройка HEALTHCHECK
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${SERVER_PORT}/api-health || exit 1

# Запуск приложения
CMD ["t-indicators"]