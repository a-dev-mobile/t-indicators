
use crate::utils::utils_http;
use axum::http::Request;

use tower_http::classify::{ServerErrorsAsFailures, SharedClassifier};
use tower_http::trace::TraceLayer;

use tower_http::cors::{Any, CorsLayer};

/// Создаёт и настраивает `TraceLayer` для логирования HTTP-запросов.
///
/// `TraceLayer` отвечает за создание спанов трассировки для каждого HTTP-запроса.
/// В данном случае, мы настраиваем спаны таким образом, чтобы исключить логирование для маршрута `/test`
/// и добавить дополнительные поля, такие как IP клиента, User-Agent и другие заголовки.
///
/// # Возвращает
///
pub fn create_trace() -> TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    impl Fn(&Request<axum::body::Body>) -> tracing::Span + Clone,
> {
    let make_span_with = TraceLayer::new_for_http().make_span_with(|request: &Request<_>| {
        // Пропускаем логирование для запроса GET /test
        if request.uri().path() == "/test" {
            return tracing::info_span!("noop"); // Возвращаем пустой Span
        }

        // Извлечение IP клиента с использованием новой функции
        let client_ip = utils_http::get_client_ip(request);

        // Извлечение User-Agent
        let user_agent = request
            .headers()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        // Извлечение Referer
        let referer = request
            .headers()
            .get("referer")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        // Извлечение Accept-Language
        let accept_language = request
            .headers()
            .get("accept-language")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        tracing::info_span!(
            "request",
            method = %request.method(),
            uri = %request.uri(),
            version = ?request.version(),
            client_ip = %client_ip,
            user_agent = %user_agent,
            referer = %referer,
            accept_language = %accept_language,
        )
    });
    make_span_with
}

pub fn create_cors() -> CorsLayer {
    // Настройка CORS
    CorsLayer::new()
        .allow_origin(Any) // Разрешить любые источники.
        .allow_methods(Any) // Разрешить любые HTTP-методы
        .allow_headers(Any) // Разрешить любые заголовки
}
