use axum::http::{HeaderMap, Request};

/// Извлекает IP-адрес клиента из заголовка `X-Forwarded-For`.
///
/// # Аргументы
///
/// * `request` - Ссылка на объект `Request`, из которого необходимо извлечь IP.
///
/// # Возвращает
///
/// * `String` - IP-адрес клиента или `"unknown"`, если не удалось определить.
pub fn get_client_ip<B>(request: &Request<B>) -> String {
    request
        .headers()
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .to_string()
}

/// Извлекает IP-адрес клиента из HeaderMap.
///
/// # Аргументы
///
/// * `headers` - Ссылка на HeaderMap с заголовками запроса
///
/// # Возвращает
///
/// * `String` - IP-адрес клиента или "unknown", если не удалось определить
pub fn get_client_ip_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .to_string()
}
