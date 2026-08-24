use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sea_orm::{DbErr, RuntimeErr};
use serde::Serialize;
use std::ops::Deref;
use thiserror::Error;

pub type ApiResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("authentication required")]
    Unauthorized,
    #[error("access denied")]
    Forbidden,
    #[error("too many authentication attempts; try again later")]
    RateLimited,
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("content integrity check failed")]
    Integrity,
    #[error("database operation failed")]
    Database(#[from] sea_orm::DbErr),
    #[error("I/O operation failed")]
    Io(#[from] std::io::Error),
    #[error("internal error")]
    Internal(#[source] anyhow::Error),
}

impl AppError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn not_found(resource: &'static str) -> Self {
        Self::NotFound(resource)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn map_constraint(
        error: sea_orm::DbErr,
        unique_message: &'static str,
        foreign_key_message: &'static str,
    ) -> Self {
        match error.sql_err() {
            Some(sea_orm::SqlErr::UniqueConstraintViolation(_)) => Self::conflict(unique_message),
            Some(sea_orm::SqlErr::ForeignKeyConstraintViolation(_)) => {
                Self::conflict(foreign_key_message)
            }
            None if is_sqlite_foreign_key_trigger(&error) => Self::conflict(foreign_key_message),
            Some(_) | None => Self::Database(error),
        }
    }

    pub fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self::Internal(error.into())
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::Integrity => StatusCode::BAD_GATEWAY,
            Self::Database(_) | Self::Io(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::RateLimited => "rate_limited",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Unavailable(_) => "unavailable",
            Self::Upstream(_) => "upstream_error",
            Self::Integrity => "integrity_error",
            Self::Database(_) | Self::Io(_) | Self::Internal(_) => "internal_error",
        }
    }
}

fn is_sqlite_foreign_key_trigger(error: &DbErr) -> bool {
    let (DbErr::Exec(RuntimeErr::SqlxError(error)) | DbErr::Query(RuntimeErr::SqlxError(error))) =
        error
    else {
        return false;
    };
    let sea_orm::sqlx::Error::Database(database_error) = error.deref() else {
        return false;
    };
    database_error.code().as_deref() == Some("1811")
        && database_error.message() == "FOREIGN KEY constraint failed"
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() {
            tracing::error!(error = ?self, "request failed");
        }
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            "The request could not be completed".to_owned()
        } else {
            self.to_string()
        };
        (
            status,
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code(),
                    message,
                },
            }),
        )
            .into_response()
    }
}
