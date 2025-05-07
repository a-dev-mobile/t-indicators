# Сборка приложения
# https://hub.docker.com/_/rust/tags
FROM rust:1.85.0-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies if needed
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev curl&& \
    rm -rf /var/lib/apt/lists/*

# Copy only files needed for dependency resolution first (for better caching)
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir -p src && \
    echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/t_indicators* target/release/t-indicators*

# Copy actual source code and rebuild
COPY .sqlx ./.sqlx
COPY src ./src
COPY config ./config

# Build application
RUN cargo build --release && \
    strip target/release/t-indicators

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies only
RUN apt-get update && \
    apt-get install -y ca-certificates curl&& \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копирование собранного бинарного файла
COPY --from=builder /app/target/release/t-indicators /app/
COPY --from=builder /app/config /app/config

# Create a non-root user to run the application
RUN useradd -m appuser && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 5005

# Настройка HEALTHCHECK
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${SERVER_PORT}/api-health || exit 1
    
# Запуск приложения
CMD ["/app/t-indicators"]