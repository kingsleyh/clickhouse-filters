use clickhouse_filters::sorting::{SortOrder, SortedColumn, Sorting};

#[test]
fn test_sorting_with_multiple_columns() {
    let sorting = Sorting::new(vec![
        SortedColumn::new("name", "asc"),
        SortedColumn::new("age", "desc"),
    ]);

    assert_eq!(sorting.columns.len(), 2);
    assert_eq!(sorting.sql, " ORDER BY age DESC, name ASC");
}

#[test]
fn test_sorting_with_single_column() {
    let sorting = Sorting::new(vec![SortedColumn::new("name", "asc")]);

    assert_eq!(sorting.columns.len(), 1);
    assert_eq!(sorting.sql, " ORDER BY name ASC");
}

#[test]
fn test_sorting_with_empty_columns() {
    let sorting = Sorting::new(vec![]);

    assert_eq!(sorting.columns.len(), 0);
    assert_eq!(sorting.sql, "");
}

#[test]
fn test_sorted_column_case_insensitive() {
    let col1 = SortedColumn::new("name", "ASC");
    let col2 = SortedColumn::new("name", "asc");
    let col3 = SortedColumn::new("name", "Asc");

    assert_eq!(col1.order, SortOrder::Asc);
    assert_eq!(col2.order, SortOrder::Asc);
    assert_eq!(col3.order, SortOrder::Asc);
}

#[test]
fn test_sorting_with_duplicated_columns() {
    // The second instance of a column should be ignored
    let sorting = Sorting::new(vec![
        SortedColumn::new("name", "asc"),
        SortedColumn::new("name", "desc"), // This should be ignored
        SortedColumn::new("age", "desc"),
    ]);

    assert_eq!(sorting.columns.len(), 2);
    assert_eq!(sorting.sql, " ORDER BY age DESC, name ASC");
}

#[test]
fn test_sorting_with_invalid_order() {
    // Invalid order should default to ASC
    let sorting = Sorting::new(vec![SortedColumn::new("name", "invalid")]);

    assert_eq!(sorting.columns.len(), 1);
    assert_eq!(sorting.sql, " ORDER BY name ASC");
}
