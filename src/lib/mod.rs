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
use crate::filtering::{FilterBuilder, FilterCondition, FilterExpression, FilterOperator};
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
            ColumnDef::UInt8(name)
            | ColumnDef::UInt16(name)
            | ColumnDef::UInt32(name)
            | ColumnDef::UInt64(name)
            | ColumnDef::UInt128(name)
            | ColumnDef::UInt256(name)
            | ColumnDef::Int8(name)
            | ColumnDef::Int16(name)
            | ColumnDef::Int32(name)
            | ColumnDef::Int64(name)
            | ColumnDef::Int128(name)
            | ColumnDef::Int256(name) => name.to_string(),

            // Numeric Types - Floating Point
            ColumnDef::Float32(name) | ColumnDef::Float64(name) => name.to_string(),

            // Date/Time Types
            ColumnDef::Date(name)
            | ColumnDef::Date32(name)
            | ColumnDef::DateTime(name)
            | ColumnDef::DateTime64(name) => name.to_string(),

            // Boolean Type
            ColumnDef::Boolean(name) => name.to_string(),

            // UUID Type
            ColumnDef::UUID(name) => name.to_string(),

            // Array Types
            ColumnDef::ArrayString(name)
            | ColumnDef::ArrayUInt8(name)
            | ColumnDef::ArrayUInt16(name)
            | ColumnDef::ArrayUInt32(name)
            | ColumnDef::ArrayUInt64(name)
            | ColumnDef::ArrayInt8(name)
            | ColumnDef::ArrayInt16(name)
            | ColumnDef::ArrayInt32(name)
            | ColumnDef::ArrayInt64(name)
            | ColumnDef::ArrayFloat32(name)
            | ColumnDef::ArrayFloat64(name) => name.to_string(),

            // Special Types
            ColumnDef::Enum8(name)
            | ColumnDef::Enum16(name)
            | ColumnDef::IPv4(name)
            | ColumnDef::IPv6(name)
            | ColumnDef::Decimal(name) => name.to_string(),

            // JSON Types
            ColumnDef::JSON(name) => name.to_string(),
        }
    }

    // Convert ColumnDef to appropriate FilterCondition
    pub fn to_filter_condition(&self, operator: &str, value: &str) -> Result<FilterCondition> {
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
            "DATE_ONLY" => FilterOperator::DateEqual,
            "DATE_RANGE" => FilterOperator::DateRange,
            "RELATIVE" => FilterOperator::RelativeDate,
            _ => return Err(eyre::eyre!("Invalid operator: {}", operator)),
        };

        // Check if operator is for NULL checks
        let is_null_check = op == FilterOperator::IsNull || op == FilterOperator::IsNotNull;

        match self {
            // String types
            ColumnDef::String(name) | ColumnDef::FixedString(name) => {
                Ok(FilterCondition::StringValue {
                    column: name.to_string(),
                    operator: op,
                    value: if is_null_check {
                        None
                    } else {
                        Some(value.to_string())
                    },
                })
            }

            // Integer types
            ColumnDef::UInt8(name) => {
                if is_null_check {
                    Ok(FilterCondition::UInt8Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    // Handle IN/NOT IN with multiple values
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    // Parse as u8
                    match value.parse::<u8>() {
                        Ok(parsed) => Ok(FilterCondition::UInt8Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for UInt8: {}", value)),
                    }
                }
            }
            ColumnDef::UInt16(name) => {
                if is_null_check {
                    Ok(FilterCondition::UInt16Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<u16>() {
                        Ok(parsed) => Ok(FilterCondition::UInt16Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for UInt16: {}", value)),
                    }
                }
            }
            ColumnDef::UInt32(name) => {
                if is_null_check {
                    Ok(FilterCondition::UInt32Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<u32>() {
                        Ok(parsed) => Ok(FilterCondition::UInt32Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for UInt32: {}", value)),
                    }
                }
            }
            ColumnDef::UInt64(name) => {
                if is_null_check {
                    Ok(FilterCondition::UInt64Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<u64>() {
                        Ok(parsed) => Ok(FilterCondition::UInt64Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for UInt64: {}", value)),
                    }
                }
            }
            ColumnDef::Int8(name) => {
                if is_null_check {
                    Ok(FilterCondition::Int8Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<i8>() {
                        Ok(parsed) => Ok(FilterCondition::Int8Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Int8: {}", value)),
                    }
                }
            }
            ColumnDef::Int16(name) => {
                if is_null_check {
                    Ok(FilterCondition::Int16Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<i16>() {
                        Ok(parsed) => Ok(FilterCondition::Int16Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Int16: {}", value)),
                    }
                }
            }
            ColumnDef::Int32(name) => {
                if is_null_check {
                    Ok(FilterCondition::Int32Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<i32>() {
                        Ok(parsed) => Ok(FilterCondition::Int32Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Int32: {}", value)),
                    }
                }
            }
            ColumnDef::Int64(name) => {
                if is_null_check {
                    Ok(FilterCondition::Int64Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::Numeric),
                    })
                } else {
                    match value.parse::<i64>() {
                        Ok(parsed) => Ok(FilterCondition::Int64Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Int64: {}", value)),
                    }
                }
            }

            // Float types
            ColumnDef::Float32(name) => {
                if is_null_check {
                    Ok(FilterCondition::Float32Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    match value.parse::<f32>() {
                        Ok(parsed) => Ok(FilterCondition::Float32Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Float32: {}", value)),
                    }
                }
            }
            ColumnDef::Float64(name) => {
                if is_null_check {
                    Ok(FilterCondition::Float64Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    match value.parse::<f64>() {
                        Ok(parsed) => Ok(FilterCondition::Float64Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid value for Float64: {}", value)),
                    }
                }
            }

            // Date/Time types
            ColumnDef::Date(name) => {
                if is_null_check {
                    Ok(FilterCondition::DateValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::DateEqual {
                    // DATE_ONLY operator
                    Ok(FilterCondition::date_only(name, value))
                } else if op == FilterOperator::DateRange {
                    // DATE_RANGE operator
                    let parts: Vec<&str> = value.split(',').collect();
                    if parts.len() == 2 {
                        Ok(FilterCondition::date_range(
                            name,
                            parts[0].trim(),
                            parts[1].trim(),
                        ))
                    } else {
                        Err(eyre::eyre!(
                            "DATE_RANGE requires two comma-separated values"
                        ))
                    }
                } else if op == FilterOperator::RelativeDate {
                    // RELATIVE operator
                    Ok(FilterCondition::relative_date(name, value))
                } else {
                    Ok(FilterCondition::DateValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }
            ColumnDef::Date32(name) => {
                if is_null_check {
                    Ok(FilterCondition::DateValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::DateEqual {
                    Ok(FilterCondition::date_only(name, value))
                } else if op == FilterOperator::DateRange {
                    let parts: Vec<&str> = value.split(',').collect();
                    if parts.len() == 2 {
                        Ok(FilterCondition::date_range(
                            name,
                            parts[0].trim(),
                            parts[1].trim(),
                        ))
                    } else {
                        Err(eyre::eyre!(
                            "DATE_RANGE requires two comma-separated values"
                        ))
                    }
                } else if op == FilterOperator::RelativeDate {
                    Ok(FilterCondition::relative_date(name, value))
                } else {
                    Ok(FilterCondition::DateValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }
            ColumnDef::DateTime(name) => {
                if is_null_check {
                    Ok(FilterCondition::DateTimeValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::DateEqual {
                    Ok(FilterCondition::date_only(name, value))
                } else if op == FilterOperator::DateRange {
                    let parts: Vec<&str> = value.split(',').collect();
                    if parts.len() == 2 {
                        Ok(FilterCondition::date_range(
                            name,
                            parts[0].trim(),
                            parts[1].trim(),
                        ))
                    } else {
                        Err(eyre::eyre!(
                            "DATE_RANGE requires two comma-separated values"
                        ))
                    }
                } else if op == FilterOperator::RelativeDate {
                    Ok(FilterCondition::relative_date(name, value))
                } else {
                    Ok(FilterCondition::DateTimeValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }
            ColumnDef::DateTime64(name) => {
                if is_null_check {
                    Ok(FilterCondition::DateTime64Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::DateEqual {
                    Ok(FilterCondition::date_only(name, value))
                } else if op == FilterOperator::DateRange {
                    let parts: Vec<&str> = value.split(',').collect();
                    if parts.len() == 2 {
                        Ok(FilterCondition::date_range(
                            name,
                            parts[0].trim(),
                            parts[1].trim(),
                        ))
                    } else {
                        Err(eyre::eyre!(
                            "DATE_RANGE requires two comma-separated values"
                        ))
                    }
                } else if op == FilterOperator::RelativeDate {
                    Ok(FilterCondition::relative_date(name, value))
                } else {
                    Ok(FilterCondition::DateTime64Value {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }

            // Boolean type
            ColumnDef::Boolean(name) => {
                if is_null_check {
                    Ok(FilterCondition::BooleanValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    match value.to_lowercase().as_str() {
                        "true" | "1" | "yes" | "y" => Ok(FilterCondition::BooleanValue {
                            column: name.to_string(),
                            operator: op,
                            value: Some(true),
                        }),
                        "false" | "0" | "no" | "n" => Ok(FilterCondition::BooleanValue {
                            column: name.to_string(),
                            operator: op,
                            value: Some(false),
                        }),
                        _ => Err(eyre::eyre!("Invalid boolean value: {}", value)),
                    }
                }
            }

            // UUID type
            ColumnDef::UUID(name) => {
                if is_null_check {
                    Ok(FilterCondition::UUIDValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else if op == FilterOperator::In || op == FilterOperator::NotIn {
                    Ok(FilterCondition::InValues {
                        column: name.to_string(),
                        operator: op,
                        values: value.split(',').map(|v| v.trim().to_string()).collect(),
                        column_type: Some(filtering::ColumnTypeInfo::UUID),
                    })
                } else {
                    Ok(FilterCondition::UUIDValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }

            // Array types
            ColumnDef::ArrayString(name) => {
                if op == FilterOperator::ArrayContains {
                    Ok(FilterCondition::ArrayContains {
                        column: name.to_string(),
                        operator: op,
                        value: value.to_string(),
                    })
                } else if op == FilterOperator::ArrayHas {
                    Ok(FilterCondition::ArrayHas {
                        column: name.to_string(),
                        operator: op,
                        value: value.to_string(),
                    })
                } else if is_null_check {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    Err(eyre::eyre!(
                        "Unsupported operator for array type: {}",
                        operator
                    ))
                }
            }
            ColumnDef::ArrayUInt8(name)
            | ColumnDef::ArrayUInt16(name)
            | ColumnDef::ArrayUInt32(name)
            | ColumnDef::ArrayUInt64(name)
            | ColumnDef::ArrayInt8(name)
            | ColumnDef::ArrayInt16(name)
            | ColumnDef::ArrayInt32(name)
            | ColumnDef::ArrayInt64(name)
            | ColumnDef::ArrayFloat32(name)
            | ColumnDef::ArrayFloat64(name) => {
                if op == FilterOperator::ArrayContains {
                    Ok(FilterCondition::ArrayContains {
                        column: name.to_string(),
                        operator: op,
                        value: value.to_string(),
                    })
                } else if op == FilterOperator::ArrayHas {
                    Ok(FilterCondition::ArrayHas {
                        column: name.to_string(),
                        operator: op,
                        value: value.to_string(),
                    })
                } else if is_null_check {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    Err(eyre::eyre!(
                        "Unsupported operator for array type: {}",
                        operator
                    ))
                }
            }

            // JSON type
            ColumnDef::JSON(name) => {
                // Extract path if provided (separated by dot or in JSONPath format)
                let mut json_path = None;
                let mut json_value = value.to_string();

                if value.contains('.') && !is_null_check {
                    let parts: Vec<&str> = value.splitn(2, '.').collect();
                    if parts.len() == 2 {
                        json_path = Some(parts[0].to_string());
                        json_value = parts[1].to_string();
                    }
                }

                if is_null_check {
                    Ok(FilterCondition::JSONValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                        path: json_path,
                    })
                } else {
                    Ok(FilterCondition::JSONValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(json_value),
                        path: json_path,
                    })
                }
            }

            // Enum types (treated as strings)
            ColumnDef::Enum8(name) | ColumnDef::Enum16(name) => {
                if is_null_check {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }

            // Network address types (treated as strings)
            ColumnDef::IPv4(name) | ColumnDef::IPv6(name) => {
                if is_null_check {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    Ok(FilterCondition::StringValue {
                        column: name.to_string(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }

            // Decimal type
            ColumnDef::Decimal(name) => {
                if is_null_check {
                    Ok(FilterCondition::Float64Value {
                        column: name.to_string(),
                        operator: op,
                        value: None,
                    })
                } else {
                    match value.parse::<f64>() {
                        Ok(parsed) => Ok(FilterCondition::Float64Value {
                            column: name.to_string(),
                            operator: op,
                            value: Some(parsed),
                        }),
                        Err(_) => Err(eyre::eyre!("Invalid decimal value: {}", value)),
                    }
                }
            }

            // Anything else - fallback to string value
            _ => {
                if is_null_check {
                    Ok(FilterCondition::StringValue {
                        column: self.get_column_name(),
                        operator: op,
                        value: None,
                    })
                } else {
                    Ok(FilterCondition::StringValue {
                        column: self.get_column_name(),
                        operator: op,
                        value: Some(value.to_string()),
                    })
                }
            }
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

    /// Create FilteringOptions from JSON filters
    pub fn from_json_filters(
        filters: &[filtering::JsonFilter],
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Result<Option<Self>> {
        if filters.is_empty() {
            return Ok(None);
        }

        let filter_builder =
            filtering::FilterBuilder::from_json_filters(filters, true, &column_defs)?;
        Ok(filter_builder
            .root
            .map(|root| Self::new(vec![root], column_defs)))
    }

    /// Convert to FilterBuilder
    pub fn to_filter_builder(&self) -> Result<filtering::FilterBuilder> {
        let mut builder = filtering::FilterBuilder::new().case_insensitive(self.case_insensitive);

        // If there are multiple expressions, wrap them in a group with AND operator
        if self.expressions.len() > 1 {
            builder = builder.group(filtering::LogicalOperator::And, self.expressions.clone());
        } else if let Some(expr) = self.expressions.first() {
            builder = builder.add_expression(expr.clone());
        }

        Ok(builder)
    }

    /// Generate SQL for this filtering option
    pub fn to_sql(&self) -> Result<String> {
        let builder = self.to_filter_builder()?;
        builder.build()
    }

    /// Create FilteringOptions from expressions with validation
    pub fn try_from_expressions(
        expressions: Vec<Result<FilterExpression, eyre::Error>>,
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Result<Option<Self>> {
        let expressions: Result<Vec<_>, _> = expressions.into_iter().collect();
        match expressions {
            Ok(exprs) if !exprs.is_empty() => Ok(Some(Self::new(exprs, column_defs))),
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }
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
        pagination: Option<PaginationOptions>,
        sorting_columns: Vec<SortedColumn>,
        filtering_options: Option<FilteringOptions>,
        column_defs: HashMap<&'static str, ColumnDef>,
    ) -> Result<ClickHouseFilters> {
        // Create sorting component
        let sorting = if sorting_columns.is_empty() {
            None
        } else {
            Some(Sorting::new(sorting_columns))
        };

        // Create pagination component
        let pagination = pagination.map(|opts| {
            pagination::Paginate::new(
                opts.current_page,
                opts.per_page,
                opts.per_page_limit,
                opts.total_records,
            )
        });

        // Create filtering component
        let filters = match filtering_options {
            Some(opts) => {
                // Convert the filtering options to a FilterBuilder
                let builder = opts.to_filter_builder()?;
                Some(builder)
            }
            None => None,
        };

        Ok(ClickHouseFilters {
            pagination,
            sorting,
            filters,
            column_defs,
        })
    }

    /// Generate the SQL for this filter
    pub fn sql(&self) -> Result<String> {
        let mut sql = String::new();

        // Add WHERE clause from filters
        if let Some(filters) = &self.filters {
            sql.push_str(&filters.build()?);
        }

        // Add ORDER BY clause
        if let Some(sorting) = &self.sorting {
            sql.push_str(&sorting.sql);
        }

        // Add LIMIT and OFFSET
        if let Some(pagination) = &self.pagination {
            sql.push(' ');
            sql.push_str(&pagination.sql);
        }

        Ok(sql)
    }

    /// Generate a SQL COUNT query for this filter
    pub fn count_sql(&self, schema: &str, table: &str) -> Result<String> {
        let mut sql = format!("SELECT COUNT(*) FROM {}.{}", schema, table);

        // Add WHERE clause from filters
        if let Some(filters) = &self.filters {
            sql.push_str(&filters.build()?);
        }

        Ok(sql)
    }

    /// Generate a complete SQL query for this filter
    pub fn query_sql(&self, schema: &str, table: &str, columns: &[&str]) -> Result<String> {
        let columns_str = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };

        let mut sql = format!("SELECT {} FROM {}.{}", columns_str, schema, table);

        // Add WHERE clause from filters
        if let Some(filters) = &self.filters {
            sql.push_str(&filters.build()?);
        }

        // Add ORDER BY clause
        if let Some(sorting) = &self.sorting {
            sql.push_str(&sorting.sql);
        }

        // Add LIMIT and OFFSET
        if let Some(pagination) = &self.pagination {
            sql.push(' ');
            sql.push_str(&pagination.sql);
        }

        Ok(sql)
    }
}
