//! Sorting module for ClickHouse SQL queries
//!
//! This module contains types and functions for generating ORDER BY clauses in ClickHouse SQL.
//! It handles multi-column sorting with ascending/descending options.
//!
//! # Example
//!
//! ```rust
//! use clickhouse_filters::sorting::{SortedColumn, Sorting};
//!
//! let sorting = Sorting::new(vec![
//!    SortedColumn::new("name", "asc"),
//!    SortedColumn::new("age", "desc"),
//! ]);
//!
//! assert_eq!(sorting.columns.len(), 2);
//! assert_eq!(sorting.sql, " ORDER BY age DESC, name ASC");
//! ```

/// SortOrder enum represents sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// SortedColumn represents a column to sort by with direction
#[derive(Debug, Clone)]
pub struct SortedColumn {
    /// Column name
    pub column: String,
    /// Sorting order
    pub order: SortOrder,
}

impl SortedColumn {
    /// Create a new SortedColumn
    pub fn new(column: &str, order: &str) -> SortedColumn {
        let order = match order.to_lowercase().as_str() {
            "asc" => SortOrder::Asc,
            "desc" => SortOrder::Desc,
            _ => SortOrder::Asc,
        };
        SortedColumn {
            column: column.to_string(),
            order,
        }
    }
}

/// Sorting represents a complete ORDER BY clause
#[derive(Debug, Clone)]
pub struct Sorting {
    /// Vector of columns to sort by
    pub columns: Vec<SortedColumn>,
    /// The generated SQL
    pub sql: String,
}

impl Sorting {
    /// Create a new Sorting from a list of SortedColumns
    pub fn new(columns: Vec<SortedColumn>) -> Sorting {
        let mut columns = columns;
        // Sort and deduplicate columns to ensure consistent ordering
        columns.sort_by(|a, b| a.column.cmp(&b.column));
        columns.dedup_by(|a, b| a.column == b.column);

        let mut sql = if !columns.is_empty() {
            " ORDER BY ".to_string()
        } else {
            "".to_string()
        };
        
        // Build the SQL ORDER BY clause
        let mut first = true;
        for column in columns.iter() {
            if first {
                first = false;
            } else {
                sql.push_str(", ");
            }
            match column.order {
                SortOrder::Asc => {
                    sql.push_str(&column.column);
                    sql.push_str(" ASC");
                }
                SortOrder::Desc => {
                    sql.push_str(&column.column);
                    sql.push_str(" DESC");
                }
            }
        }
        
        Sorting { columns, sql }
    }
}