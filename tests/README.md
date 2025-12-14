# Test Suite for Shopzada Data Pipeline

This directory contains the test suite for validating the Shopzada data pipeline across all 5 scenarios defined in `test_scenarios.md`.

## Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures and database configuration
├── test_scenarios.py        # Main test file for all 5 scenarios
└── test_data/
    ├── scenario1_new_daily_orders/
    │   ├── order_data_test.csv
    │   ├── line_item_data_prices_test.csv
    │   ├── line_item_data_products_test.csv
    │   └── order_delays_test.csv
    ├── scenario2_new_customer_product/
    │   ├── order_data_test.csv
    │   ├── line_item_data_prices_test.csv
    │   ├── line_item_data_products_test.csv
    │   ├── user_data_test.json
    │   └── product_data_test.csv
    ├── scenario3_late_campaign/
    │   ├── order_data_test.csv
    │   ├── line_item_data_prices_test.csv
    │   ├── line_item_data_products_test.csv
    │   ├── transactional_campaign_data_phase1.csv
    │   └── campaign_data_phase2.csv
    └── scenario4_data_quality/
        ├── order_data_test.csv
        ├── line_item_data_prices_test.csv
        ├── line_item_data_products_test.csv
        └── order_delays_test.csv
```

## Test Scenarios

### Scenario 1: New Daily Orders File (End-to-End Pipeline Test)

**Purpose:** Validate incremental batch load updates the warehouse correctly.

| Test                                   | Description                                           |
| -------------------------------------- | ----------------------------------------------------- |
| `test_order_data_file_structure`       | Verify order file has correct columns and structure   |
| `test_line_items_match_orders`         | Verify line items reference valid orders              |
| `test_prices_and_products_alignment`   | Verify prices and products have matching counts       |
| `test_delays_reference_valid_orders`   | Verify delay records reference valid orders           |
| `test_expected_sales_calculation`      | Calculate and verify expected sales totals (~$617.91) |
| `test_transaction_date_is_target_date` | Verify all orders are for 2024-12-14                  |

**Test Data:**

- 5 orders for date 2024-12-14
- 8 line items across orders
- 2 delayed orders

---

### Scenario 2: New Customer and New Product (Dimension Creation Test)

**Purpose:** Validate new dimension members are created with valid surrogate keys.

| Test                                       | Description                                         |
| ------------------------------------------ | --------------------------------------------------- |
| `test_new_user_ids_are_unique`             | Verify new user IDs (USER99999, USER99998)          |
| `test_new_product_ids_are_unique`          | Verify new product IDs (PRODUCT99999, PRODUCT99998) |
| `test_user_data_has_required_fields`       | Verify user dimension attributes                    |
| `test_product_catalog_has_required_fields` | Verify product dimension attributes                 |
| `test_orders_link_to_new_entities`         | Verify orders correctly link to new dimensions      |

**Test Data:**

- 2 new customers (USER99999, USER99998)
- 2 new products (PRODUCT99999, PRODUCT99998)
- Full dimension attributes (name, email, category, brand, etc.)

---

### Scenario 3: Late & Missing Campaign Data (Unknown/Not Applicable Handling)

**Purpose:** Validate handling of late-arriving dimension data.

| Test                                             | Description                                      |
| ------------------------------------------------ | ------------------------------------------------ |
| `test_phase1_has_missing_campaigns`              | Verify phase 1 references non-existent campaigns |
| `test_phase2_provides_campaign_definitions`      | Verify phase 2 defines all missing campaigns     |
| `test_campaign_definitions_have_required_fields` | Verify campaign attributes                       |
| `test_availed_flag_is_valid`                     | Verify availed flag is 0 or 1                    |
| `test_orders_have_campaign_links`                | Verify orders are properly linked to campaigns   |

**Test Data:**

- Phase 1: 3 orders referencing CAMPAIGN-MISSING-001/002/003
- Phase 2: Campaign dimension data defining those campaigns

---

### Scenario 4: Data Quality Failure & Error Handling

**Purpose:** Validate handling of invalid/dirty data.

| Test                               | Description                                     |
| ---------------------------------- | ----------------------------------------------- |
| `test_identify_valid_orders`       | Identify 5 valid orders from mixed data         |
| `test_identify_invalid_orders`     | Identify 5 invalid orders (null IDs, bad dates) |
| `test_identify_invalid_line_items` | Detect invalid prices, quantities               |
| `test_orphan_line_items_detection` | Detect line items for non-existent orders       |
| `test_invalid_delay_values`        | Detect negative and non-numeric delays          |

**Test Data Issues:**

- Missing order_id
- Missing/null user_id
- Invalid date format ("INVALID-DATE-FORMAT")
- Non-numeric price ("INVALID_PRICE")
- Zero/negative prices
- Invalid quantity format
- Orphan records

---

### Scenario 5: Performance & Aggregation Consistency

**Purpose:** Validate calculation accuracy and consistency.

| Test                                | Description                               |
| ----------------------------------- | ----------------------------------------- |
| `test_line_item_totals_calculation` | Verify order totals match expected values |
| `test_order_count_by_status`        | Verify 2 delayed, 3 on-time orders        |
| `test_quantity_aggregation`         | Verify total quantity = 17                |
| `test_unique_product_count`         | Verify 8 unique products                  |
| `test_avg_order_value_calculation`  | Verify AOV = $123.58                      |

---

## Running Tests

### Run All Tests (excluding database)

```bash
pytest tests/test_scenarios.py -v -k "not Database"
```

### Run Specific Scenario

```bash
# Scenario 1
pytest tests/test_scenarios.py -v -k "Scenario1"

# Scenario 2
pytest tests/test_scenarios.py -v -k "Scenario2"

# Scenario 3
pytest tests/test_scenarios.py -v -k "Scenario3"

# Scenario 4
pytest tests/test_scenarios.py -v -k "Scenario4"

# Scenario 5
pytest tests/test_scenarios.py -v -k "Scenario5"
```

### Run Database Integration Tests

```bash
# Requires running PostgreSQL database
pytest tests/test_scenarios.py -v -m db
```

### Run with Coverage

```bash
pytest tests/test_scenarios.py --cov=. --cov-report=html
```

## Test Data Files Summary

| Scenario | File                                   | Description                        | Records |
| -------- | -------------------------------------- | ---------------------------------- | ------- |
| 1        | order_data_test.csv                    | 5 new daily orders                 | 5       |
| 1        | line_item_data_prices_test.csv         | Prices for orders                  | 8       |
| 1        | line_item_data_products_test.csv       | Products for orders                | 8       |
| 1        | order_delays_test.csv                  | Delay information                  | 2       |
| 2        | order_data_test.csv                    | Orders with new customers          | 2       |
| 2        | user_data_test.json                    | New customer dimension data        | 2       |
| 2        | product_data_test.csv                  | New product dimension data         | 2       |
| 3        | transactional_campaign_data_phase1.csv | Orders with missing campaigns      | 3       |
| 3        | campaign_data_phase2.csv               | Late-arriving campaign definitions | 3       |
| 4        | order_data_test.csv                    | Mix of valid/invalid orders        | 10      |
| 4        | line_item_data_prices_test.csv         | Mix of valid/invalid prices        | 10      |
| 4        | order_delays_test.csv                  | Mix of valid/invalid delays        | 4       |

## Expected Test Results

```
============================= test session starts =============================
collected 36 items / 6 deselected / 30 selected

tests/test_scenarios.py::TestScenario1NewDailyOrders::test_order_data_file_structure PASSED
tests/test_scenarios.py::TestScenario1NewDailyOrders::test_line_items_match_orders PASSED
tests/test_scenarios.py::TestScenario1NewDailyOrders::test_prices_and_products_alignment PASSED
tests/test_scenarios.py::TestScenario1NewDailyOrders::test_delays_reference_valid_orders PASSED
tests/test_scenarios.py::TestScenario1NewDailyOrders::test_expected_sales_calculation PASSED
tests/test_scenarios.py::TestScenario1NewDailyOrders::test_transaction_date_is_target_date PASSED
tests/test_scenarios.py::TestScenario2NewCustomerProduct::test_new_user_ids_are_unique PASSED
tests/test_scenarios.py::TestScenario2NewCustomerProduct::test_new_product_ids_are_unique PASSED
tests/test_scenarios.py::TestScenario2NewCustomerProduct::test_user_data_has_required_fields PASSED
tests/test_scenarios.py::TestScenario2NewCustomerProduct::test_product_catalog_has_required_fields PASSED
tests/test_scenarios.py::TestScenario2NewCustomerProduct::test_orders_link_to_new_entities PASSED
tests/test_scenarios.py::TestScenario3LateCampaignData::test_phase1_has_missing_campaigns PASSED
tests/test_scenarios.py::TestScenario3LateCampaignData::test_phase2_provides_campaign_definitions PASSED
tests/test_scenarios.py::TestScenario3LateCampaignData::test_campaign_definitions_have_required_fields PASSED
tests/test_scenarios.py::TestScenario3LateCampaignData::test_availed_flag_is_valid PASSED
tests/test_scenarios.py::TestScenario3LateCampaignData::test_orders_have_campaign_links PASSED
tests/test_scenarios.py::TestScenario4DataQuality::test_identify_valid_orders PASSED
tests/test_scenarios.py::TestScenario4DataQuality::test_identify_invalid_orders PASSED
tests/test_scenarios.py::TestScenario4DataQuality::test_identify_invalid_line_items PASSED
tests/test_scenarios.py::TestScenario4DataQuality::test_orphan_line_items_detection PASSED
tests/test_scenarios.py::TestScenario4DataQuality::test_invalid_delay_values PASSED
tests/test_scenarios.py::TestScenario5PerformanceAggregation::test_line_item_totals_calculation PASSED
tests/test_scenarios.py::TestScenario5PerformanceAggregation::test_order_count_by_status PASSED
tests/test_scenarios.py::TestScenario5PerformanceAggregation::test_quantity_aggregation PASSED
tests/test_scenarios.py::TestScenario5PerformanceAggregation::test_unique_product_count PASSED
tests/test_scenarios.py::TestScenario5PerformanceAggregation::test_avg_order_value_calculation PASSED
tests/test_scenarios.py::TestTestDataFilesExist::test_scenario1_files_exist PASSED
tests/test_scenarios.py::TestTestDataFilesExist::test_scenario2_files_exist PASSED
tests/test_scenarios.py::TestTestDataFilesExist::test_scenario3_files_exist PASSED
tests/test_scenarios.py::TestTestDataFilesExist::test_scenario4_files_exist PASSED

======================= 30 passed, 6 deselected in 0.77s ======================
```
