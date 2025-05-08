use clickhouse_filters::pagination::{Paginate, Pagination};

#[test]
fn test_pagination_new() {
    let pagination = Pagination::new(1, 10, 100, 1000);
    
    assert_eq!(pagination.current_page, 1);
    assert_eq!(pagination.previous_page, 1);
    assert_eq!(pagination.next_page, 2);
    assert_eq!(pagination.total_pages, 100);
    assert_eq!(pagination.per_page, 10);
    assert_eq!(pagination.total_records, 1000);
}

#[test]
fn test_pagination_previous_page() {
    // When on page 1, previous should still be 1
    let pagination = Pagination::new(1, 10, 100, 1000);
    assert_eq!(pagination.previous_page, 1);
    
    // When on page 2, previous should be 1
    let pagination = Pagination::new(2, 10, 100, 1000);
    assert_eq!(pagination.previous_page, 1);
    
    // When on page 50, previous should be 49
    let pagination = Pagination::new(50, 10, 100, 1000);
    assert_eq!(pagination.previous_page, 49);
}

#[test]
fn test_pagination_next_page() {
    // When on last page, next should still be last page
    let pagination = Pagination::new(100, 10, 100, 1000);
    assert_eq!(pagination.next_page, 100);
    
    // When on second-to-last page, next should be last page
    let pagination = Pagination::new(99, 10, 100, 1000);
    assert_eq!(pagination.next_page, 100);
    
    // When on first page, next should be 2
    let pagination = Pagination::new(1, 10, 100, 1000);
    assert_eq!(pagination.next_page, 2);
}

#[test]
fn test_paginate_new() {
    let paginate = Paginate::new(1, 10, 10, 1000);
    
    assert_eq!(paginate.pagination.current_page, 1);
    assert_eq!(paginate.pagination.previous_page, 1);
    assert_eq!(paginate.pagination.next_page, 2);
    assert_eq!(paginate.pagination.total_pages, 100);
    assert_eq!(paginate.pagination.per_page, 10);
    assert_eq!(paginate.pagination.total_records, 1000);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
}

#[test]
fn test_paginate_with_invalid_per_page() {
    // per_page should be capped at per_page_limit
    let paginate = Paginate::new(1, 20, 10, 1000);
    assert_eq!(paginate.pagination.per_page, 10);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
    
    // per_page should be at least 1
    let paginate = Paginate::new(1, 0, 10, 1000);
    assert_eq!(paginate.pagination.per_page, 10);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
}

#[test]
fn test_paginate_with_invalid_current_page() {
    // current_page should be at least 1
    let paginate = Paginate::new(0, 10, 10, 1000);
    assert_eq!(paginate.pagination.current_page, 1);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
    
    // current_page should be capped at total_pages
    let paginate = Paginate::new(101, 10, 10, 1000);
    assert_eq!(paginate.pagination.current_page, 100);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 990");
}

#[test]
fn test_paginate_with_zero_total_records() {
    let paginate = Paginate::new(1, 10, 10, 0);
    assert_eq!(paginate.pagination.total_pages, 0);
    assert_eq!(paginate.pagination.current_page, 1);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
}

#[test]
fn test_paginate_offset_calculation() {
    // Page 1 should have offset 0
    let paginate = Paginate::new(1, 10, 10, 1000);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 0");
    
    // Page 2 should have offset 10
    let paginate = Paginate::new(2, 10, 10, 1000);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 10");
    
    // Page 10 should have offset 90
    let paginate = Paginate::new(10, 10, 10, 1000);
    assert_eq!(paginate.sql, "LIMIT 10 OFFSET 90");
    
    // Different page size: page 2 with 20 per page should have offset 20
    let paginate = Paginate::new(2, 20, 30, 1000);
    assert_eq!(paginate.sql, "LIMIT 20 OFFSET 20");
}