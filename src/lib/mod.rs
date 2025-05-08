//! ClickHouse Filters
//!
//! This crate provides a powerful, type-safe, and flexible way to build ClickHouse SQL
//! for pagination, sorting, and filtering. It's designed to work with the ClickHouse database
//! while maintaining API compatibility with the pg_filters crate.
//!
//! # Examples
//!
//! ```rust,ignore
//! use clickhouse_filters::{ClickHouseFilters, PaginationOptions, FilteringOptions};
//! use clickhouse_filters::filtering::{FilterCondition, FilterExpression, FilterOperator};
//! use clickhouse_filters::sorting::SortedColumn;
//! use std::collections::HashMap;
//!
//! // Define column types
//! let mut columns = HashMap::new();
//! columns.insert("name", ColumnDef::String("name"));
//! columns.insert("age", ColumnDef::UInt32("age"));
//!
//! // Create filters
//! let filters = ClickHouseFilters::new(
//!     Some(PaginationOptions {
//!         current_page: 1,
//!         per_page: 10,
//!         per_page_limit: 10,
//!         total_records: 1000,
//!     }),
//!     vec![SortedColumn::new("name", "asc")],
//!     Some(FilteringOptions::new(
//!         vec![FilterExpression::Condition(FilterCondition::StringValue {
//!             column: "name".to_string(),
//!             operator: FilterOperator::Equal,
//!             value: Some("John".to_string()),
//!         })],
//!         columns.clone(),
//!     )),
//!     columns,
//! )?;
//!
//! // Generate SQL
//! let sql = filters.sql()?;
//! ```

use eyre::Result;
use std::collections::HashMap;

// Public modules
pub mod filtering;
pub mod pagination;
pub mod sorting;

// Import key types from submodules
use crate::filtering::{
    FilterBuilder, FilterCondition, FilterExpression, FilterOperator,
};
use crate::pagination::Paginate;
use crate::sorting::{SortedColumn, Sorting};

/// ColumnDef enum represents different ClickHouse column types
///
/// This is used to provide type-aware filtering and ensure correct SQL is generated
/// for different data types.
#[derive(Debug, Clone)]
pub enum ColumnDef {
    // String Types
    String(&'static str),
    FixedString(&'static str),
    
    // Numeric Types - Integers
    UInt8(&'static str),
    UInt16(&'static str),
    UInt32(&'static str),
    UInt64(&'static str),
    UInt128(&'static str),
    UInt256(&'static str),
    Int8(&'static str),
    Int16(&'static str),
    Int32(&'static str),
    Int64(&'static str),
    Int128(&'static str),
    Int256(&'static str),
    
    // Numeric Types - Floating Point
    Float32(&'static str),
    Float64(&'static str),
    
    // Date/Time Types
    Date(&'static str),
    Date32(&'static str),
    DateTime(&'static str),
    DateTime64(&'static str),
    
    // Boolean Type
    Boolean(&'static str),
    
    // UUID Type
    UUID(&'static str),
    
    // Array Types
    ArrayString(&'static str),
    ArrayUInt8(&'static str),
    ArrayUInt16(&'static str),
    ArrayUInt32(&'static str),
    ArrayUInt64(&'static str),
    ArrayInt8(&'static str),
    ArrayInt16(&'static str),
    ArrayInt32(&'static str),
    ArrayInt64(&'static str),
    ArrayFloat32(&'static str),
    ArrayFloat64(&'static str),
    
    // Special Types
    Enum8(&'static str),
    Enum16(&'static str),
    IPv4(&'static str),
    IPv6(&'static str),
    Decimal(&'static str),
    
    // JSON Types
    JSON(&'static str),
}

/// Placeholder implementation (to be expanded)
impl ColumnDef {
    pub fn get_column_name(&self) -> String {
        match self {
            // String Types
            ColumnDef::String(name) | ColumnDef::FixedString(name) => name.to_string(),
            
            // Numeric Types - Integers
            ColumnDef::UInt8(name) | ColumnDef::UInt16(name) | ColumnDef::UInt32(name) |
            ColumnDef::UInt64(name) | ColumnDef::UInt128(name) | ColumnDef::UInt256(name) |
            ColumnDef::Int8(name) | ColumnDef::Int16(name) | ColumnDef::Int32(name) |
            ColumnDef::Int64(name) | ColumnDef::Int128(name) | ColumnDef::Int256(name) => name.to_string(),
            
            // Numeric Types - Floating Point
            ColumnDef::Float32(name) | ColumnDef::Float64(name) => name.to_string(),
            
            // Date/Time Types
            ColumnDef::Date(name) | ColumnDef::Date32(name) | 
            ColumnDef::DateTime(name) | ColumnDef::DateTime64(name) => name.to_string(),
            
            // Boolean Type
            ColumnDef::Boolean(name) => name.to_string(),
            
            // UUID Type
            ColumnDef::UUID(name) => name.to_string(),
            
            // Array Types
            ColumnDef::ArrayString(name) | ColumnDef::ArrayUInt8(name) | 
            ColumnDef::ArrayUInt16(name) | ColumnDef::ArrayUInt32(name) | 
            ColumnDef::ArrayUInt64(name) | ColumnDef::ArrayInt8(name) | 
            ColumnDef::ArrayInt16(name) | ColumnDef::ArrayInt32(name) | 
            ColumnDef::ArrayInt64(name) | ColumnDef::ArrayFloat32(name) | 
            ColumnDef::ArrayFloat64(name) => name.to_string(),
            
            // Special Types
            ColumnDef::Enum8(name) | ColumnDef::Enum16(name) | 
            ColumnDef::IPv4(name) | ColumnDef::IPv6(name) | 
            ColumnDef::Decimal(name) => name.to_string(),
            
            // JSON Types
            ColumnDef::JSON(name) => name.to_string(),
        }
    }
    
    // To be implemented: method to convert to filter condition
    pub fn to_filter_condition(&self, operator: &str, value: &str) -> Result<FilterCondition> {
        // This will be implemented more fully later
        let op = match operator.to_uppercase().as_str() {
            "=" => FilterOperator::Equal,
            "!=" => FilterOperator::NotEqual,
            ">" => FilterOperator::GreaterThan,
            ">=" => FilterOperator::GreaterThanOrEqual,
            "<" => FilterOperator::LessThan,
            "<=" => FilterOperator::LessThanOrEqual,
            "LIKE" => FilterOperator::Like,
            "NOT LIKE" => FilterOperator::NotLike,
            "IN" => FilterOperator::In,
            "NOT IN" => FilterOperator::NotIn,
            "IS NULL" => FilterOperator::IsNull,
            "IS NOT NULL" => FilterOperator::IsNotNull,
            "STARTS WITH" => FilterOperator::StartsWith,
            "ENDS WITH" => FilterOperator::EndsWith,
            "ARRAY CONTAINS" => FilterOperator::ArrayContains,
            "ARRAY HAS" => FilterOperator::ArrayHas,
            _ => return Err(eyre::eyre!("Invalid operator: {}", operator)),
        };
        
        // Temporary implementation - will be expanded later
        match self {
            ColumnDef::String(name) => Ok(FilterCondition::StringValue {
                column: name.to_string(),
                operator: op,
                value: if operator == "IS NULL" || operator == "IS NOT NULL" {
                    None
                } else {
                    Some(value.to_string())
                },
            }),
            _ => Err(eyre::eyre!("Not implemented yet")),
        }
    }
}

/// Pagination options for ClickHouse queries
#[derive(Debug, Clone)]
pub struct PaginationOptions {
    pub current_page: i64,
    pub per_page: i64,
    pub per_page_limit: i64,
    pub total_records: i64,
}

impl PaginationOptions {
    pub fn new(current_page: i64, per_page: i64, per_page_limit: i64, total_records: i64) -> Self {
        Self {
            current_page,
            per_page,
            per_page_limit,
            total_records,
        }
    }
}

/// Filtering options for ClickHouse queries
#[derive(Clone)]
pub struct FilteringOptions {
    pub expressions: Vec<FilterExpression>,
    pub case_insensitive: bool,
    pub column_defs: HashMap<&'static str, ColumnDef>,
}

impl FilteringOptions {
    pub fn new(
        expressions: Vec<FilterExpression>,
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Self {
        Self {
            expressions,
            case_insensitive: true,
            column_defs,
        }
    }

    pub fn case_sensitive(
        expressions: Vec<FilterExpression>,
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Self {
        Self {
            expressions,
            case_insensitive: false,
            column_defs,
        }
    }
    
    // Will be implemented: from_json_filters method
    // Will be implemented: to_filter_builder method
}

/// Main struct for ClickHouse filtering, sorting, and pagination
#[derive(Debug, Clone)]
pub struct ClickHouseFilters {
    pub pagination: Option<Paginate>,
    pub sorting: Option<Sorting>,
    pub filters: Option<FilterBuilder>,
    pub column_defs: HashMap<&'static str, ColumnDef>,
}

impl ClickHouseFilters {
    /// Create a new ClickHouseFilters instance
    ///
    /// This is the main entry point for creating filters.
    pub fn new(
        _pagination: Option<PaginationOptions>,
        sorting_columns: Vec<SortedColumn>,
        _filtering_options: Option<FilteringOptions>,
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Result<ClickHouseFilters> {
        // Placeholder implementation
        Ok(ClickHouseFilters {
            pagination: None,
            sorting: Some(Sorting::new(sorting_columns)),
            filters: None,
            column_defs,
        })
    }

    /// Generate the SQL for this filter
    pub fn sql(&self) -> Result<String> {
        // Placeholder implementation
        let mut sql = String::new();

        // Add more components as they are implemented
        if let Some(sorting) = &self.sorting {
            sql.push_str(&sorting.sql);
        }

        Ok(sql)
    }

    /// Generate a SQL COUNT query for this filter
    pub fn count_sql(&self, schema: &str, table: &str) -> Result<String> {
        // Placeholder implementation
        Ok(format!("SELECT COUNT(*) FROM {}.{}", schema, table))
    }
}