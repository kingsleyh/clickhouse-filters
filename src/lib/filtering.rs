//! Filtering module for ClickHouse SQL queries
//!
//! This module contains types and functions for generating WHERE clauses in ClickHouse SQL.
//! It's designed to support complex filtering expressions with AND/OR conditions and various
//! operators for different data types.

use eyre::Result;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;

/// Column type information
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnTypeInfo {
    String,
    Numeric,
    UUID,
    Date,
    Boolean,
    Array,
    JSON,
    Other,
}

/// Logical operators for combining filter expressions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
}

impl LogicalOperator {
    pub fn as_sql(&self) -> &'static str {
        match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        }
    }
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator_str = match self {
            LogicalOperator::And => "AND",
            LogicalOperator::Or => "OR",
        };
        write!(f, "{}", operator_str)
    }
}

/// Filter operators for comparison
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOperator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    StartsWith,
    EndsWith,
    // ClickHouse-specific array operators
    ArrayContains,
    ArrayHas,  // Similar to PostgreSQL's @> but with different syntax in ClickHouse
    ArrayAll,  // Check if all elements match a condition
    ArrayAny,  // Check if any elements match a condition
    // ClickHouse-specific date operators
    DateEqual,
    DateRange,
    RelativeDate,
}

impl FilterOperator {
    pub fn as_sql(&self) -> &'static str {
        match self {
            FilterOperator::Equal => "=",
            FilterOperator::NotEqual => "!=",
            FilterOperator::GreaterThan => ">",
            FilterOperator::GreaterThanOrEqual => ">=",
            FilterOperator::LessThan => "<",
            FilterOperator::LessThanOrEqual => "<=",
            FilterOperator::Like => "LIKE",
            FilterOperator::NotLike => "NOT LIKE",
            FilterOperator::In => "IN",
            FilterOperator::NotIn => "NOT IN",
            FilterOperator::IsNull => "IS NULL",
            FilterOperator::IsNotNull => "IS NOT NULL",
            FilterOperator::StartsWith => "LIKE",  // Will need special handling
            FilterOperator::EndsWith => "LIKE",    // Will need special handling
            FilterOperator::ArrayContains => "hasAll",  // ClickHouse function
            FilterOperator::ArrayHas => "has",     // ClickHouse function
            FilterOperator::ArrayAll => "ALL",     // ClickHouse ALL
            FilterOperator::ArrayAny => "ANY",     // ClickHouse ANY
            FilterOperator::DateEqual => "=",      // Will need special handling
            FilterOperator::DateRange => "BETWEEN",
            FilterOperator::RelativeDate => ">",   // Will need special handling
        }
    }
    
    pub fn format_value(&self, value: &str) -> String {
        match self {
            FilterOperator::StartsWith => format!("{}%", value),
            FilterOperator::EndsWith => format!("%{}", value),
            _ => value.to_string(),
        }
    }
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator_str = self.as_sql();
        write!(f, "{}", operator_str)
    }
}

/// Filter expression - can be a condition or a group
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpression {
    Condition(FilterCondition),
    Group {
        operator: LogicalOperator,
        expressions: Vec<FilterExpression>,
    },
}

/// JSON filter structure for API usage
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonFilter {
    pub n: String,         // name/column
    pub f: String,         // filter operator
    pub v: String,         // value
    pub c: Option<String>, // optional connector (AND/OR)
}

impl FilterExpression {
    // Placeholder implementation - to be expanded
    pub fn to_sql(&self, case_insensitive: bool) -> Result<String> {
        match self {
            FilterExpression::Condition(condition) => condition.to_sql(case_insensitive),
            FilterExpression::Group {
                operator,
                expressions,
            } => {
                if expressions.is_empty() {
                    return Ok(String::new());
                }

                let conditions: Result<Vec<String>> = expressions
                    .iter()
                    .map(|expr| expr.to_sql(case_insensitive))
                    .collect();

                let conditions = conditions?;
                Ok(format!(
                    "({})",
                    conditions.join(&format!(" {} ", operator.as_sql()))
                ))
            }
        }
    }

    /// Helper to create AND group
    pub fn and(expressions: Vec<FilterExpression>) -> Self {
        FilterExpression::Group {
            operator: LogicalOperator::And,
            expressions,
        }
    }

    /// Helper to create OR group
    pub fn or(expressions: Vec<FilterExpression>) -> Self {
        FilterExpression::Group {
            operator: LogicalOperator::Or,
            expressions,
        }
    }
}

impl fmt::Display for FilterExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterExpression::Condition(condition) => write!(f, "{}", condition),
            FilterExpression::Group {
                operator,
                expressions,
            } => {
                write!(
                    f,
                    "({} {})",
                    operator,
                    expressions
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(&format!(" {} ", operator))
                )
            }
        }
    }
}

/// Date range type for date filtering
#[derive(Debug, Clone, PartialEq)]
pub enum DateRangeType {
    /// Exact timestamp match
    Exact(String),
    /// Match entire day
    DateOnly(String),
    /// Custom date range
    Range { start: String, end: String },
    /// Relative date expression
    Relative(String),
}

/// Filter condition - represents a single comparison
#[derive(Debug, Clone, PartialEq)]
pub enum FilterCondition {
    // String Types
    StringValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    FixedStringValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    
    // Numeric types
    UInt8Value {
        column: String,
        operator: FilterOperator,
        value: Option<u8>,
    },
    UInt16Value {
        column: String,
        operator: FilterOperator,
        value: Option<u16>,
    },
    UInt32Value {
        column: String,
        operator: FilterOperator,
        value: Option<u32>,
    },
    UInt64Value {
        column: String,
        operator: FilterOperator,
        value: Option<u64>,
    },
    Int8Value {
        column: String,
        operator: FilterOperator,
        value: Option<i8>,
    },
    Int16Value {
        column: String,
        operator: FilterOperator,
        value: Option<i16>,
    },
    Int32Value {
        column: String,
        operator: FilterOperator,
        value: Option<i32>,
    },
    Int64Value {
        column: String,
        operator: FilterOperator,
        value: Option<i64>,
    },
    Float32Value {
        column: String,
        operator: FilterOperator,
        value: Option<f32>,
    },
    Float64Value {
        column: String,
        operator: FilterOperator,
        value: Option<f64>,
    },
    
    // Date/Time Types
    DateValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    DateTimeValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    DateTime64Value {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    
    // Date Range
    DateRange {
        column: String,
        range_type: DateRangeType,
    },
    
    // Boolean Type
    BooleanValue {
        column: String,
        operator: FilterOperator,
        value: Option<bool>,
    },
    
    // UUID Type
    UUIDValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
    },
    
    // Multi-value conditions for IN/NOT IN
    InValues {
        column: String,
        operator: FilterOperator,
        values: Vec<String>,
        column_type: Option<ColumnTypeInfo>,
    },
    
    // Array Types
    ArrayContains {
        column: String,
        operator: FilterOperator,
        value: String,
    },
    ArrayHas {
        column: String,
        operator: FilterOperator,
        value: String,
    },
    
    // JSON Type
    JSONValue {
        column: String,
        operator: FilterOperator,
        value: Option<String>,
        path: Option<String>,
    },
}

// Placeholder implementation - will be expanded
impl FilterCondition {
    // Helper function to format values consistently
    fn format_value<T: fmt::Display>(
        column: &str,
        operator: &FilterOperator,
        value: Option<T>,
    ) -> String {
        match value {
            Some(v) => format!("{} {} {}", column, operator.as_sql(), v),
            None => format!("{} {}", column, operator.as_sql()),
        }
    }

    // Helper function for string values
    fn format_string_value(column: &str, operator: &FilterOperator, value: Option<&str>) -> String {
        match value {
            Some(v) => format!(
                "{} {} '{}'",
                column,
                operator.as_sql(),
                v.replace('\'', "''")
            ),
            None => format!("{} {}", column, operator.as_sql()),
        }
    }

    // Escape single quotes in string values
    fn escape_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    // Complete to_sql implementation with all supported conditions
    pub fn to_sql(&self, case_insensitive: bool) -> Result<String> {
        match self {
            // String Types
            FilterCondition::StringValue {
                column,
                operator,
                value,
            } | FilterCondition::FixedStringValue {
                column,
                operator,
                value,
            } => match operator {
                FilterOperator::Equal | FilterOperator::NotEqual => match value {
                    Some(v) => {
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) {} lower('{}')",
                                column,
                                operator.as_sql(),
                                Self::escape_string(v)
                            ))
                        } else {
                            Ok(format!(
                                "{} {} '{}'",
                                column,
                                operator.as_sql(),
                                Self::escape_string(v)
                            ))
                        }
                    }
                    None => Ok(format!("{} {}", column, operator.as_sql())),
                },
                FilterOperator::Like | FilterOperator::NotLike => match value {
                    Some(v) => {
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) {} lower('{}')",
                                column,
                                operator.as_sql(),
                                Self::escape_string(v)
                            ))
                        } else {
                            Ok(format!(
                                "{} {} '{}'",
                                column,
                                operator.as_sql(),
                                Self::escape_string(v)
                            ))
                        }
                    }
                    None => Ok(format!("{} {}", column, operator.as_sql())),
                },
                FilterOperator::StartsWith => match value {
                    Some(v) => {
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) LIKE lower('{}%')",
                                column,
                                Self::escape_string(v)
                            ))
                        } else {
                            Ok(format!(
                                "{} LIKE '{}%'",
                                column,
                                Self::escape_string(v)
                            ))
                        }
                    }
                    None => Ok(format!("{} LIKE '%'", column)),
                },
                FilterOperator::EndsWith => match value {
                    Some(v) => {
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) LIKE lower('%{}')",
                                column,
                                Self::escape_string(v)
                            ))
                        } else {
                            Ok(format!(
                                "{} LIKE '%{}'",
                                column,
                                Self::escape_string(v)
                            ))
                        }
                    }
                    None => Ok(format!("{} LIKE '%'", column)),
                },
                FilterOperator::In => match value {
                    Some(v) => {
                        let values = v
                            .split(',')
                            .map(|item| item.trim())
                            .collect::<Vec<_>>();
                        let formatted_values = values
                            .iter()
                            .map(|val| format!("'{}'", Self::escape_string(val)))
                            .collect::<Vec<_>>()
                            .join(", ");
                        
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) IN ({})",
                                column,
                                values
                                    .iter()
                                    .map(|val| format!("lower('{}')", Self::escape_string(val)))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ))
                        } else {
                            Ok(format!("{} IN ({})", column, formatted_values))
                        }
                    }
                    None => Err(eyre::eyre!("IN operator requires values")),
                },
                FilterOperator::NotIn => match value {
                    Some(v) => {
                        let values = v
                            .split(',')
                            .map(|item| item.trim())
                            .collect::<Vec<_>>();
                        let formatted_values = values
                            .iter()
                            .map(|val| format!("'{}'", Self::escape_string(val)))
                            .collect::<Vec<_>>()
                            .join(", ");
                        
                        if case_insensitive {
                            Ok(format!(
                                "lower({}) NOT IN ({})",
                                column,
                                values
                                    .iter()
                                    .map(|val| format!("lower('{}')", Self::escape_string(val)))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ))
                        } else {
                            Ok(format!("{} NOT IN ({})", column, formatted_values))
                        }
                    }
                    None => Err(eyre::eyre!("NOT IN operator requires values")),
                },
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for string type")),
            },
            
            // Numeric integer types (similar implementation for all integer types)
            FilterCondition::UInt8Value {
                column,
                operator,
                value: _,
            } | FilterCondition::UInt16Value {
                column,
                operator,
                value: _,
            } | FilterCondition::UInt32Value {
                column,
                operator,
                value: _,
            } | FilterCondition::UInt64Value {
                column,
                operator,
                value: _,
            } | FilterCondition::Int8Value {
                column,
                operator,
                value: _,
            } | FilterCondition::Int16Value {
                column,
                operator,
                value: _,
            } | FilterCondition::Int32Value {
                column,
                operator,
                value: _,
            } | FilterCondition::Int64Value {
                column,
                operator,
                value: _,
            } => match operator {
                FilterOperator::Equal
                | FilterOperator::NotEqual
                | FilterOperator::GreaterThan
                | FilterOperator::GreaterThanOrEqual
                | FilterOperator::LessThan
                | FilterOperator::LessThanOrEqual => {
                    // Get the value based on the enum variant
                    let value_str = match self {
                        FilterCondition::UInt8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt64Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int64Value { value, .. } => value.map(|v| v.to_string()),
                        _ => None,
                    };
                    
                    match value_str {
                        Some(v) => Ok(format!("{} {} {}", column, operator.as_sql(), v)),
                        None => Ok(format!("{} {}", column, operator.as_sql())),
                    }
                }
                FilterOperator::In => {
                    // Get the values based on the enum variant
                    let value_str = match self {
                        FilterCondition::UInt8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt64Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int64Value { value, .. } => value.map(|v| v.to_string()),
                        _ => None,
                    };
                    
                    match value_str {
                        Some(v) => {
                            let values = v
                                .split(',')
                                .map(|item| item.trim())
                                .collect::<Vec<_>>()
                                .join(", ");
                            Ok(format!("{} IN ({})", column, values))
                        }
                        None => Err(eyre::eyre!("IN operator requires values")),
                    }
                }
                FilterOperator::NotIn => {
                    // Get the values based on the enum variant
                    let value_str = match self {
                        FilterCondition::UInt8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::UInt64Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int8Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int16Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Int64Value { value, .. } => value.map(|v| v.to_string()),
                        _ => None,
                    };
                    
                    match value_str {
                        Some(v) => {
                            let values = v
                                .split(',')
                                .map(|item| item.trim())
                                .collect::<Vec<_>>()
                                .join(", ");
                            Ok(format!("{} NOT IN ({})", column, values))
                        }
                        None => Err(eyre::eyre!("NOT IN operator requires values")),
                    }
                }
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for integer type")),
            },
            
            // Floating point types
            FilterCondition::Float32Value {
                column,
                operator,
                value: _,
            } | FilterCondition::Float64Value {
                column,
                operator,
                value: _,
            } => match operator {
                FilterOperator::Equal
                | FilterOperator::NotEqual
                | FilterOperator::GreaterThan
                | FilterOperator::GreaterThanOrEqual
                | FilterOperator::LessThan
                | FilterOperator::LessThanOrEqual => {
                    // Get the value based on the enum variant
                    let value_str = match self {
                        FilterCondition::Float32Value { value, .. } => value.map(|v| v.to_string()),
                        FilterCondition::Float64Value { value, .. } => value.map(|v| v.to_string()),
                        _ => None,
                    };
                    
                    match value_str {
                        Some(v) => Ok(format!("{} {} {}", column, operator.as_sql(), v)),
                        None => Ok(format!("{} {}", column, operator.as_sql())),
                    }
                }
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for float type")),
            },
            
            // Date/Time Types
            FilterCondition::DateValue {
                column,
                operator,
                value: _,
            } | FilterCondition::DateTimeValue {
                column,
                operator,
                value: _,
            } | FilterCondition::DateTime64Value {
                column,
                operator,
                value: _,
            } => match operator {
                FilterOperator::Equal
                | FilterOperator::NotEqual
                | FilterOperator::GreaterThan
                | FilterOperator::GreaterThanOrEqual
                | FilterOperator::LessThan
                | FilterOperator::LessThanOrEqual => {
                    // Get the value based on the enum variant
                    let value_str = match self {
                        FilterCondition::DateValue { value, .. } => value.clone(),
                        FilterCondition::DateTimeValue { value, .. } => value.clone(),
                        FilterCondition::DateTime64Value { value, .. } => value.clone(),
                        _ => None,
                    };
                    
                    match value_str {
                        Some(v) => Ok(format!("{} {} '{}'", column, operator.as_sql(), v)),
                        None => Ok(format!("{} {}", column, operator.as_sql())),
                    }
                }
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for date/time type")),
            },
            
            // Date Range specific handling
            FilterCondition::DateRange {
                column,
                range_type,
            } => match range_type {
                DateRangeType::Exact(timestamp) => {
                    Ok(format!("{} = '{}'", column, timestamp))
                }
                DateRangeType::DateOnly(date) => {
                    // In ClickHouse we can use toDate function
                    Ok(format!("toDate({}) = toDate('{}')", column, date))
                }
                DateRangeType::Range { start, end } => {
                    Ok(format!("{} BETWEEN '{}' AND '{}'", column, start, end))
                }
                DateRangeType::Relative(expr) => {
                    // For ClickHouse we directly pass the expression
                    Ok(format!("{} > {}", column, expr))
                }
            },
            
            // Boolean Type
            FilterCondition::BooleanValue {
                column,
                operator,
                value,
            } => match operator {
                FilterOperator::Equal | FilterOperator::NotEqual => match value {
                    Some(v) => {
                        // ClickHouse uses 0/1 for boolean values
                        let bool_val = if *v { 1 } else { 0 };
                        Ok(format!("{} {} {}", column, operator.as_sql(), bool_val))
                    }
                    None => Ok(format!("{} {}", column, operator.as_sql())),
                },
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for boolean type")),
            },
            
            // UUID Type
            FilterCondition::UUIDValue {
                column,
                operator,
                value,
            } => match operator {
                FilterOperator::Equal | FilterOperator::NotEqual => match value {
                    Some(v) => Ok(format!("{} {} '{}'", column, operator.as_sql(), v)),
                    None => Ok(format!("{} {}", column, operator.as_sql())),
                },
                FilterOperator::In => match value {
                    Some(v) => {
                        let values = v
                            .split(',')
                            .map(|item| format!("'{}'", item.trim()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        Ok(format!("{} IN ({})", column, values))
                    }
                    None => Err(eyre::eyre!("IN operator requires values")),
                },
                FilterOperator::NotIn => match value {
                    Some(v) => {
                        let values = v
                            .split(',')
                            .map(|item| format!("'{}'", item.trim()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        Ok(format!("{} NOT IN ({})", column, values))
                    }
                    None => Err(eyre::eyre!("NOT IN operator requires values")),
                },
                FilterOperator::IsNull => Ok(format!("{} IS NULL", column)),
                FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", column)),
                _ => Err(eyre::eyre!("Unsupported operator for UUID type")),
            },
            
            // Array Types
            FilterCondition::ArrayContains {
                column,
                operator: _,
                value,
            } => {
                // In ClickHouse, we use `hasAll` function for array containment
                let values = value
                    .split(',')
                    .map(|s| format!("'{}'", s.trim().replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("hasAll({}, array[{}])", column, values))
            },
            FilterCondition::ArrayHas {
                column,
                operator: _,
                value,
            } => {
                // In ClickHouse, we use `has` function for checking if array contains a value
                Ok(format!("has({}, '{}')", column, value.replace('\'', "''")))
            },
            
            // JSON Type
            FilterCondition::JSONValue {
                column,
                operator,
                value,
                path,
            } => {
                // Use ClickHouse's JSONExtract functions based on the path
                let json_column = match path {
                    Some(p) => format!("JSONExtractString({}, '{}')", column, p),
                    None => column.clone(),
                };
                
                match operator {
                    FilterOperator::Equal | FilterOperator::NotEqual => match value {
                        Some(v) => {
                            if case_insensitive && operator == &FilterOperator::Equal {
                                Ok(format!(
                                    "lower({}) = lower('{}')",
                                    json_column,
                                    Self::escape_string(v)
                                ))
                            } else if case_insensitive && operator == &FilterOperator::NotEqual {
                                Ok(format!(
                                    "lower({}) != lower('{}')",
                                    json_column,
                                    Self::escape_string(v)
                                ))
                            } else {
                                Ok(format!(
                                    "{} {} '{}'",
                                    json_column,
                                    operator.as_sql(),
                                    Self::escape_string(v)
                                ))
                            }
                        }
                        None => Ok(format!("{} {}", json_column, operator.as_sql())),
                    },
                    FilterOperator::IsNull => Ok(format!("{} IS NULL", json_column)),
                    FilterOperator::IsNotNull => Ok(format!("{} IS NOT NULL", json_column)),
                    _ => Err(eyre::eyre!("Unsupported operator for JSON type")),
                }
            },
            
            // For InValues (complex type handling for IN/NOT IN)
            FilterCondition::InValues {
                column,
                operator,
                values,
                column_type,
            } => {
                let is_text = match column_type {
                    Some(ColumnTypeInfo::String) => true,
                    _ => false,
                };
                
                let formatted_values = if is_text {
                    values
                        .iter()
                        .map(|v| {
                            if case_insensitive {
                                format!("lower('{}')", Self::escape_string(v))
                            } else {
                                format!("'{}'", Self::escape_string(v))
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                } else {
                    values
                        .iter()
                        .map(|v| {
                            // Check if the value is numeric
                            if v.parse::<f64>().is_ok() {
                                v.to_string()
                            } else {
                                format!("'{}'", Self::escape_string(v))
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                
                let column_name = if case_insensitive && is_text {
                    format!("lower({})", column)
                } else {
                    column.to_string()
                };
                
                match operator {
                    FilterOperator::In => Ok(format!("{} IN ({})", column_name, formatted_values)),
                    FilterOperator::NotIn => {
                        Ok(format!("{} NOT IN ({})", column_name, formatted_values))
                    }
                    _ => Err(eyre::eyre!("Invalid operator for InValues condition")),
                }
            },
        }
    }
    
    // Convenience constructors for different types
    
    // String type
    pub fn string(column: &str, operator: FilterOperator, value: Option<&str>) -> Self {
        FilterCondition::StringValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
        }
    }
    
    // Fixed string type
    pub fn fixed_string(column: &str, operator: FilterOperator, value: Option<&str>) -> Self {
        FilterCondition::FixedStringValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
        }
    }
    
    // UInt8 type
    pub fn uint8(column: &str, operator: FilterOperator, value: Option<u8>) -> Self {
        FilterCondition::UInt8Value {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // UInt32 type
    pub fn uint32(column: &str, operator: FilterOperator, value: Option<u32>) -> Self {
        FilterCondition::UInt32Value {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // Int32 type
    pub fn int32(column: &str, operator: FilterOperator, value: Option<i32>) -> Self {
        FilterCondition::Int32Value {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // Int64 type
    pub fn int64(column: &str, operator: FilterOperator, value: Option<i64>) -> Self {
        FilterCondition::Int64Value {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // Float64 type
    pub fn float64(column: &str, operator: FilterOperator, value: Option<f64>) -> Self {
        FilterCondition::Float64Value {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // Date type
    pub fn date(column: &str, operator: FilterOperator, value: Option<&str>) -> Self {
        FilterCondition::DateValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
        }
    }
    
    // DateTime type
    pub fn date_time(column: &str, operator: FilterOperator, value: Option<&str>) -> Self {
        FilterCondition::DateTimeValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
        }
    }
    
    // Boolean type
    pub fn boolean(column: &str, operator: FilterOperator, value: Option<bool>) -> Self {
        FilterCondition::BooleanValue {
            column: column.to_string(),
            operator,
            value,
        }
    }
    
    // UUID type
    pub fn uuid(column: &str, operator: FilterOperator, value: Option<&str>) -> Self {
        FilterCondition::UUIDValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
        }
    }
    
    // JSON type
    pub fn json(column: &str, operator: FilterOperator, value: Option<&str>, path: Option<&str>) -> Self {
        FilterCondition::JSONValue {
            column: column.to_string(),
            operator,
            value: value.map(ToString::to_string),
            path: path.map(ToString::to_string),
        }
    }
    
    // Array contains (checks if array contains ALL specified values)
    pub fn array_contains(column: &str, values: &str) -> Self {
        FilterCondition::ArrayContains {
            column: column.to_string(),
            operator: FilterOperator::ArrayContains,
            value: values.to_string(),
        }
    }
    
    // Array has (checks if array contains ANY of the specified values)
    pub fn array_has(column: &str, value: &str) -> Self {
        FilterCondition::ArrayHas {
            column: column.to_string(),
            operator: FilterOperator::ArrayHas,
            value: value.to_string(),
        }
    }
    
    // Date range helpers
    
    pub fn date_exact(column: &str, timestamp: &str) -> Self {
        FilterCondition::DateRange {
            column: column.to_string(),
            range_type: DateRangeType::Exact(timestamp.to_string()),
        }
    }
    
    pub fn date_only(column: &str, date: &str) -> Self {
        FilterCondition::DateRange {
            column: column.to_string(),
            range_type: DateRangeType::DateOnly(date.to_string()),
        }
    }
    
    pub fn date_range(column: &str, start: &str, end: &str) -> Self {
        FilterCondition::DateRange {
            column: column.to_string(),
            range_type: DateRangeType::Range {
                start: start.to_string(),
                end: end.to_string(),
            },
        }
    }
    
    pub fn relative_date(column: &str, expr: &str) -> Self {
        FilterCondition::DateRange {
            column: column.to_string(),
            range_type: DateRangeType::Relative(expr.to_string()),
        }
    }
    
    // IN values with type information
    pub fn in_values(column: &str, operator: FilterOperator, values: Vec<String>, column_type: Option<ColumnTypeInfo>) -> Self {
        FilterCondition::InValues {
            column: column.to_string(),
            operator,
            values,
            column_type,
        }
    }
    
    // More constructors will be added for other types
}

impl fmt::Display for FilterCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_sql(false) {
            Ok(sql) => write!(f, "{}", sql),
            Err(err) => write!(f, "Error: {}", err),
        }
    }
}

/// Filter builder for creating complex filter expressions
#[derive(Debug, Clone, PartialEq)]
pub struct FilterBuilder {
    pub root: Option<FilterExpression>,
    pub case_insensitive: bool,
}

impl FilterBuilder {
    pub fn new() -> Self {
        Self {
            root: None,
            case_insensitive: false,
        }
    }

    pub fn case_insensitive(mut self, value: bool) -> Self {
        self.case_insensitive = value;
        self
    }

    pub fn add_condition(self, condition: FilterCondition) -> Self {
        self.add_expression(FilterExpression::Condition(condition))
    }

    pub fn add_expression(mut self, expression: FilterExpression) -> Self {
        match &self.root {
            None => {
                self.root = Some(expression);
            }
            Some(existing) => {
                self.root = Some(FilterExpression::Group {
                    operator: LogicalOperator::And,
                    expressions: vec![existing.clone(), expression],
                });
            }
        }
        self
    }

    pub fn group(mut self, operator: LogicalOperator, expressions: Vec<FilterExpression>) -> Self {
        let group = FilterExpression::Group {
            operator,
            expressions,
        };
        match &self.root {
            None => {
                self.root = Some(group);
            }
            Some(existing) => {
                self.root = Some(FilterExpression::Group {
                    operator: LogicalOperator::And,
                    expressions: vec![existing.clone(), group],
                });
            }
        }
        self
    }
    
    /// Create a FilterBuilder from JSON filters
    pub fn from_json_filters(
        filters: &[JsonFilter],
        case_insensitive: bool,
        column_defs: &std::collections::HashMap<&'static str, crate::ColumnDef>,
    ) -> Result<Self> {
        use LogicalOperator::{And, Or};
        
        if filters.is_empty() {
            return Ok(Self::new().case_insensitive(case_insensitive));
        }
        
        let mut builder = Self::new().case_insensitive(case_insensitive);
        let mut current_group: Option<(LogicalOperator, Vec<FilterExpression>)> = None;
        let mut last_connector: Option<LogicalOperator> = None;
        
        for filter in filters {
            // Get column definition
            let column_def = column_defs
                .get(filter.n.as_str())
                .ok_or_else(|| eyre::eyre!("Column not found: {}", filter.n))?;
            
            // Parse operator
            let operator = &filter.f;
            
            // Create the condition from column definition
            let condition = column_def.to_filter_condition(operator, &filter.v)?;
            let expression = FilterExpression::Condition(condition);
            
            // Handle connector logic
            match &filter.c {
                Some(connector) => {
                    let op = match connector.to_uppercase().as_str() {
                        "AND" => And,
                        "OR" => Or,
                        _ => And, // Default to AND
                    };
                    
                    match &mut current_group {
                        None => {
                            // Start new group
                            current_group = Some((op, vec![expression]));
                            last_connector = Some(op);
                        }
                        Some((current_op, expressions)) => {
                            if *current_op == op {
                                // Add to current group
                                expressions.push(expression);
                            } else {
                                // Finish current group and start new one
                                let group = FilterExpression::Group {
                                    operator: *current_op,
                                    expressions: expressions.clone(),
                                };
                                
                                // Add the group to the builder
                                builder = builder.add_expression(group);
                                
                                // Start new group
                                current_group = Some((op, vec![expression]));
                            }
                            last_connector = Some(op);
                        }
                    }
                }
                None => {
                    // No connector specified
                    match &mut current_group {
                        Some((_, expressions)) if last_connector.is_some() => {
                            // Add to current group
                            expressions.push(expression);
                        }
                        _ => {
                            // No current group or no last connector, add directly to builder
                            builder = builder.add_expression(expression);
                        }
                    }
                }
            }
        }
        
        // Add any remaining group
        if let Some((op, expressions)) = current_group {
            if expressions.len() > 1 {
                let group = FilterExpression::Group {
                    operator: op,
                    expressions,
                };
                builder = builder.add_expression(group);
            } else if let Some(expr) = expressions.first() {
                builder = builder.add_expression(expr.clone());
            }
        }
        
        Ok(builder)
    }
    
    pub fn build(&self) -> Result<String> {
        match &self.root {
            None => Ok(String::new()),
            Some(expression) => {
                let sql = expression.to_sql(self.case_insensitive)?;
                if sql.is_empty() {
                    Ok(String::new())
                } else {
                    Ok(format!(" WHERE {}", sql))
                }
            }
        }
    }
}

// Helper function for operator parsing
pub fn parse_operator(op: &str) -> FilterOperator {
    match op.to_uppercase().as_str() {
        "LIKE" => FilterOperator::Like,
        "=" => FilterOperator::Equal,
        "!=" => FilterOperator::NotEqual,
        ">" => FilterOperator::GreaterThan,
        ">=" => FilterOperator::GreaterThanOrEqual,
        "<" => FilterOperator::LessThan,
        "<=" => FilterOperator::LessThanOrEqual,
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
        _ => FilterOperator::Equal,
    }
}

impl Default for FilterBuilder {
    fn default() -> Self {
        Self::new()
    }
}