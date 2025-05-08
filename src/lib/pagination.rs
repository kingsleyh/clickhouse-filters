//! Pagination module for ClickHouse SQL queries
//!
//! This module contains types and functions for generating LIMIT and OFFSET clauses
//! in ClickHouse SQL as well as calculating pagination metadata.
//!
//! # Example
//!
//! ```rust
//! use clickhouse_filters::pagination::Paginate;
//!
//! let paginate = Paginate::new(1, 10, 10, 1000);
//! assert_eq!(paginate.pagination.current_page, 1);
//! assert_eq!(paginate.pagination.previous_page, 1);
//! assert_eq!(paginate.pagination.next_page, 2);
//! assert_eq!(paginate.pagination.total_pages, 100);
//! assert_eq!(paginate.pagination.per_page, 10);
//! assert_eq!(paginate.pagination.total_records, 1000);
//! assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
//! ```

/// Pagination metadata
#[derive(Debug, Clone)]
pub struct Pagination {
    pub current_page: i64,
    pub previous_page: i64,
    pub next_page: i64,
    pub total_pages: i64,
    pub per_page: i64,
    pub total_records: i64,
}

impl Pagination {
    /// Create new pagination metadata
    pub fn new(
        current_page: i64,
        per_page: i64,
        total_pages: i64,
        total_records: i64,
    ) -> Pagination {
        // Calculate previous page (never less than 1)
        let previous_page = if current_page > 1 {
            current_page - 1
        } else {
            1
        };

        // Calculate next page (never more than total pages)
        let next_page = if current_page < total_pages && total_pages > 0 {
            current_page + 1
        } else if current_page < total_pages {
            total_pages
        } else {
            current_page
        };

        Pagination {
            current_page,
            previous_page,
            next_page,
            total_pages,
            per_page,
            total_records,
        }
    }
}

/// SQL pagination with metadata
#[derive(Debug, Clone)]
pub struct Paginate {
    /// Pagination metadata
    pub pagination: Pagination,
    /// SQL LIMIT and OFFSET clause
    pub sql: String,
}

impl Paginate {
    /// Create new pagination with SQL
    pub fn new(
        current_page: i64,
        per_page: i64,
        per_page_limit: i64,
        total_records: i64,
    ) -> Paginate {
        // Validate per_page_limit (default to 10 if invalid)
        let per_page_limit = if per_page_limit > 0 {
            per_page_limit
        } else {
            10
        };

        // Ensure per_page is within limits
        let per_page = if per_page > per_page_limit {
            per_page_limit
        } else {
            per_page
        };
        let per_page = if per_page > 0 { per_page } else { 10 };

        // Validate total_records
        let total_records = if total_records > 0 { total_records } else { 0 };

        // Calculate total pages
        let total_pages = if total_records > 0 {
            (total_records as f64 / per_page as f64).ceil() as i64
        } else {
            0
        };

        // Validate current_page
        let current_page = if current_page < 1 { 1 } else { current_page };
        let current_page = if current_page > total_pages && total_pages > 0 {
            total_pages
        } else {
            current_page
        };

        // Calculate LIMIT and OFFSET
        let limit = per_page;
        let offset = (limit * current_page) - limit;

        // Create pagination metadata
        let pagination = Pagination::new(current_page, per_page, total_pages, total_records);

        // Generate SQL
        let sql = format!("LIMIT {} OFFSET {}", limit, offset);

        Paginate { pagination, sql }
    }
}
