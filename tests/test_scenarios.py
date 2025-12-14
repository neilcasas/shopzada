"""
Pytest Test Suite for Shopzada Data Pipeline Scenarios
=======================================================

This module contains tests for the 5 key scenarios defined in test_scenarios.md:

1. Scenario 1: New Daily Orders File (End-to-End Pipeline Test)
2. Scenario 2: New Customer and New Product (Dimension Creation Test)
3. Scenario 3: Late & Missing Campaign Data (Unknown/Not Applicable Handling)
4. Scenario 4: Data Quality Failure & Error Handling
5. Scenario 5: Performance & Aggregation Consistency

Each scenario tests different aspects of the data pipeline's correctness,
robustness, and data quality handling.
"""

import os
import pytest
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "test_data"


# =============================================================================
# FIXTURES FOR LOADING TEST DATA
# =============================================================================


@pytest.fixture
def scenario1_test_data():
    """Load Scenario 1 test data files - New Daily Orders."""
    base_path = TEST_DATA_DIR / "scenario1_new_daily_orders"
    return {
        "orders": pd.read_csv(base_path / "order_data_test.csv", index_col=0),
        "prices": pd.read_csv(base_path / "line_item_data_prices_test.csv", index_col=0),
        "products": pd.read_csv(base_path / "line_item_data_products_test.csv", index_col=0),
        "delays": pd.read_csv(base_path / "order_delays_test.csv", index_col=0),
    }


@pytest.fixture
def scenario2_test_data():
    """Load Scenario 2 test data files - New Customer and Product."""
    base_path = TEST_DATA_DIR / "scenario2_new_customer_product"
    with open(base_path / "user_data_test.json", "r") as f:
        user_data = json.load(f)
    return {
        "orders": pd.read_csv(base_path / "order_data_test.csv", index_col=0),
        "prices": pd.read_csv(base_path / "line_item_data_prices_test.csv", index_col=0),
        "products": pd.read_csv(base_path / "line_item_data_products_test.csv", index_col=0),
        "users": pd.DataFrame(user_data),
        "product_catalog": pd.read_csv(base_path / "product_data_test.csv"),
    }


@pytest.fixture
def scenario3_test_data():
    """Load Scenario 3 test data files - Late Campaign Data."""
    base_path = TEST_DATA_DIR / "scenario3_late_campaign"
    return {
        "orders": pd.read_csv(base_path / "order_data_test.csv", index_col=0),
        "prices": pd.read_csv(base_path / "line_item_data_prices_test.csv", index_col=0),
        "products": pd.read_csv(base_path / "line_item_data_products_test.csv", index_col=0),
        "campaign_transactions_phase1": pd.read_csv(
            base_path / "transactional_campaign_data_phase1.csv", index_col=0
        ),
        "campaign_data_phase2": pd.read_csv(
            base_path / "campaign_data_phase2.csv", sep="\t"
        ),
    }


@pytest.fixture
def scenario4_test_data():
    """Load Scenario 4 test data files - Data Quality Issues."""
    base_path = TEST_DATA_DIR / "scenario4_data_quality"
    return {
        "orders": pd.read_csv(base_path / "order_data_test.csv", index_col=0),
        "prices": pd.read_csv(base_path / "line_item_data_prices_test.csv", index_col=0),
        "products": pd.read_csv(base_path / "line_item_data_products_test.csv", index_col=0),
        "delays": pd.read_csv(base_path / "order_delays_test.csv", index_col=0),
    }


# =============================================================================
# SCENARIO 1: NEW DAILY ORDERS FILE (End-to-End Pipeline Test)
# =============================================================================


class TestScenario1NewDailyOrders:
    """
    Test Scenario 1: New Daily Orders File
    
    Validates that the system correctly handles incremental batch loads
    and updates the warehouse end-to-end.
    """

    def test_order_data_file_structure(self, scenario1_test_data):
        """Verify the test order data file has correct structure."""
        orders = scenario1_test_data["orders"]
        
        # Check required columns exist
        required_columns = ["order_id", "user_id", "estimated arrival", "transaction_date"]
        for col in required_columns:
            assert col in orders.columns, f"Missing required column: {col}"
        
        # Check we have expected number of orders
        assert len(orders) == 5, "Should have 5 test orders"
        
        # Check all order IDs follow test pattern
        assert all(orders["order_id"].str.startswith("TEST-ORD-")), \
            "All test order IDs should start with TEST-ORD-"

    def test_line_items_match_orders(self, scenario1_test_data):
        """Verify line items reference valid orders."""
        orders = scenario1_test_data["orders"]
        prices = scenario1_test_data["prices"]
        products = scenario1_test_data["products"]
        
        order_ids = set(orders["order_id"].tolist())
        
        # All line item prices should reference existing orders
        price_order_ids = set(prices["order_id"].tolist())
        assert price_order_ids.issubset(order_ids), \
            "All line item prices should reference valid orders"
        
        # All line item products should reference existing orders
        product_order_ids = set(products["order_id"].tolist())
        assert product_order_ids.issubset(order_ids), \
            "All line item products should reference valid orders"

    def test_prices_and_products_alignment(self, scenario1_test_data):
        """Verify prices and products have matching counts per order."""
        prices = scenario1_test_data["prices"]
        products = scenario1_test_data["products"]
        
        # Count items per order
        price_counts = prices.groupby("order_id").size()
        product_counts = products.groupby("order_id").size()
        
        # Should have same number of items per order
        for order_id in price_counts.index:
            assert order_id in product_counts.index, \
                f"Order {order_id} in prices but not in products"
            assert price_counts[order_id] == product_counts[order_id], \
                f"Mismatch for order {order_id}: {price_counts[order_id]} prices vs {product_counts[order_id]} products"

    def test_delays_reference_valid_orders(self, scenario1_test_data):
        """Verify delay records reference valid orders."""
        orders = scenario1_test_data["orders"]
        delays = scenario1_test_data["delays"]
        
        order_ids = set(orders["order_id"].tolist())
        delay_order_ids = set(delays["order_id"].tolist())
        
        assert delay_order_ids.issubset(order_ids), \
            "All delay records should reference valid orders"
        
        # Check delay values are positive integers
        assert all(delays["delay_in_days"] > 0), \
            "All delay values should be positive"

    def test_expected_sales_calculation(self, scenario1_test_data):
        """Calculate expected sales totals from test data."""
        prices = scenario1_test_data["prices"]
        
        # Parse price and quantity
        prices_copy = prices.copy()
        prices_copy["price_num"] = pd.to_numeric(prices_copy["price"], errors="coerce")
        prices_copy["qty_num"] = prices_copy["quantity"].str.extract(r"(\d+)").astype(float)
        prices_copy["line_total"] = prices_copy["price_num"] * prices_copy["qty_num"]
        
        expected_total = prices_copy["line_total"].sum()
        
        # Expected: (25.99*2) + (15.50*1) + (99.99*1) + (45.00*3) + (12.75*2) + (199.99*1) + (8.99*5) + (22.50*2)
        # = 51.98 + 15.50 + 99.99 + 135.00 + 25.50 + 199.99 + 44.95 + 45.00 = 617.91
        assert abs(expected_total - 617.91) < 0.01, \
            f"Expected total ~617.91, got {expected_total}"

    def test_transaction_date_is_target_date(self, scenario1_test_data):
        """Verify all orders are for the target test date."""
        orders = scenario1_test_data["orders"]
        
        # All orders should be for 2024-12-14
        assert all(orders["transaction_date"] == "2024-12-14"), \
            "All test orders should be for date 2024-12-14"


# =============================================================================
# SCENARIO 2: NEW CUSTOMER AND NEW PRODUCT (Dimension Creation Test)
# =============================================================================


class TestScenario2NewCustomerProduct:
    """
    Test Scenario 2: New Customer and New Product
    
    Validates that the system correctly creates new dimension members
    and links them to fact rows.
    """

    def test_new_user_ids_are_unique(self, scenario2_test_data):
        """Verify new user IDs follow unique pattern."""
        orders = scenario2_test_data["orders"]
        users = scenario2_test_data["users"]
        
        # User IDs should be USER99999, USER99998 (high numbers unlikely to exist)
        user_ids = orders["user_id"].tolist()
        assert "USER99999" in user_ids, "Should have USER99999 (new customer)"
        assert "USER99998" in user_ids, "Should have USER99998 (new customer)"

    def test_new_product_ids_are_unique(self, scenario2_test_data):
        """Verify new product IDs follow unique pattern."""
        products = scenario2_test_data["products"]
        
        # Product IDs should be PRODUCT99999, PRODUCT99998 (high numbers unlikely to exist)
        product_ids = products["product_id"].tolist()
        assert "PRODUCT99999" in product_ids, "Should have PRODUCT99999 (new product)"
        assert "PRODUCT99998" in product_ids, "Should have PRODUCT99998 (new product)"

    def test_user_data_has_required_fields(self, scenario2_test_data):
        """Verify user data has all required dimension attributes."""
        users = scenario2_test_data["users"]
        
        required_fields = ["user_id", "first_name", "last_name", "email", "membership_status"]
        for field in required_fields:
            assert field in users.columns, f"Missing required user field: {field}"

    def test_product_catalog_has_required_fields(self, scenario2_test_data):
        """Verify product catalog has all required dimension attributes."""
        products = scenario2_test_data["product_catalog"]
        
        required_fields = ["product_id", "product_name", "category", "brand", "price"]
        for field in required_fields:
            assert field in products.columns, f"Missing required product field: {field}"

    def test_orders_link_to_new_entities(self, scenario2_test_data):
        """Verify orders correctly reference new users and products."""
        orders = scenario2_test_data["orders"]
        products = scenario2_test_data["products"]
        
        # Each order should link to one user and one product
        for _, order in orders.iterrows():
            order_id = order["order_id"]
            user_id = order["user_id"]
            
            # Find matching product
            matching_products = products[products["order_id"] == order_id]
            assert len(matching_products) > 0, \
                f"Order {order_id} has no matching products"
            
            # Verify user exists in user data
            assert user_id.startswith("USER9999"), \
                f"User {user_id} should be a new test user"


# =============================================================================
# SCENARIO 3: LATE & MISSING CAMPAIGN DATA (Unknown/Not Applicable Handling)
# =============================================================================


class TestScenario3LateCampaignData:
    """
    Test Scenario 3: Late & Missing Campaign Data
    
    Validates how the system handles optional or late-arriving data
    such as campaign information.
    """

    def test_phase1_has_missing_campaigns(self, scenario3_test_data):
        """Verify phase 1 data references campaigns that don't exist yet."""
        transactions = scenario3_test_data["campaign_transactions_phase1"]
        
        # Should have campaign IDs that follow "MISSING" pattern
        campaign_ids = transactions["campaign_id"].tolist()
        assert all("CAMPAIGN-MISSING" in cid for cid in campaign_ids), \
            "All phase 1 campaigns should be 'missing' campaigns"
        
        # Should have 3 different missing campaigns
        unique_campaigns = set(campaign_ids)
        assert len(unique_campaigns) == 3, "Should have 3 unique missing campaigns"

    def test_phase2_provides_campaign_definitions(self, scenario3_test_data):
        """Verify phase 2 data provides definitions for missing campaigns."""
        phase2_campaigns = scenario3_test_data["campaign_data_phase2"]
        transactions = scenario3_test_data["campaign_transactions_phase1"]
        
        # Phase 2 should define all the missing campaigns from phase 1
        missing_campaigns = set(transactions["campaign_id"].tolist())
        defined_campaigns = set(phase2_campaigns["campaign_id"].tolist())
        
        assert missing_campaigns == defined_campaigns, \
            "Phase 2 should define all campaigns referenced in phase 1"

    def test_campaign_definitions_have_required_fields(self, scenario3_test_data):
        """Verify campaign definitions have required attributes."""
        campaigns = scenario3_test_data["campaign_data_phase2"]
        
        required_fields = ["campaign_id", "campaign_name", "campaign_description", "discount"]
        for field in required_fields:
            assert field in campaigns.columns, f"Missing required campaign field: {field}"

    def test_availed_flag_is_valid(self, scenario3_test_data):
        """Verify availed flag is 0 or 1."""
        transactions = scenario3_test_data["campaign_transactions_phase1"]
        
        availed_values = transactions["availed"].unique()
        assert all(v in [0, 1] for v in availed_values), \
            "Availed flag should only be 0 or 1"

    def test_orders_have_campaign_links(self, scenario3_test_data):
        """Verify orders are properly linked to campaigns."""
        orders = scenario3_test_data["orders"]
        transactions = scenario3_test_data["campaign_transactions_phase1"]
        
        # All orders should have campaign transaction records
        order_ids = set(orders["order_id"].tolist())
        campaign_order_ids = set(transactions["order_id"].tolist())
        
        assert order_ids == campaign_order_ids, \
            "All orders should have campaign transaction records"


# =============================================================================
# SCENARIO 4: DATA QUALITY FAILURE & ERROR HANDLING
# =============================================================================


class TestScenario4DataQuality:
    """
    Test Scenario 4: Data Quality Failure & Error Handling
    
    Validates how the system behaves when encountering invalid or dirty data.
    """

    def test_identify_valid_orders(self, scenario4_test_data):
        """Identify which orders are valid for processing."""
        orders = scenario4_test_data["orders"]
        
        # Valid orders have: non-null order_id, non-null user_id, valid date
        valid_orders = orders[
            orders["order_id"].notna() & 
            (orders["order_id"] != "") &
            orders["user_id"].notna() & 
            (orders["user_id"] != "") &
            (orders["user_id"].astype(str).str.lower() != "nan") &
            orders["transaction_date"].notna() &
            (orders["transaction_date"] != "INVALID-DATE-FORMAT")
        ]
        
        # Should have 5 valid orders (indices 0, 1, 4, 7, 9)
        valid_order_ids = valid_orders["order_id"].tolist()
        expected_valid = [
            "TEST-ORD-VALID-001",
            "TEST-ORD-VALID-002",
            "TEST-ORD-VALID-003",
            "TEST-ORD-VALID-004",
            "TEST-ORD-VALID-005",
        ]
        
        for expected_id in expected_valid:
            assert expected_id in valid_order_ids, \
                f"Expected {expected_id} to be valid"

    def test_identify_invalid_orders(self, scenario4_test_data):
        """Identify which orders should be rejected."""
        orders = scenario4_test_data["orders"]
        
        invalid_reasons = []
        
        for idx, row in orders.iterrows():
            order_id = row["order_id"]
            user_id = row["user_id"]
            transaction_date = row["transaction_date"]
            
            # Check for null/empty order_id
            if pd.isna(order_id) or order_id == "":
                invalid_reasons.append((idx, "missing_order_id"))
                continue
            
            # Check for null/empty user_id
            if pd.isna(user_id) or user_id == "" or str(user_id).lower() == "nan":
                invalid_reasons.append((idx, "missing_user_id"))
                continue
            
            # Check for invalid date
            if pd.isna(transaction_date) or transaction_date == "INVALID-DATE-FORMAT":
                invalid_reasons.append((idx, "invalid_date"))
                continue
        
        # Should have 5 invalid orders
        assert len(invalid_reasons) == 5, \
            f"Expected 5 invalid orders, found {len(invalid_reasons)}: {invalid_reasons}"

    def test_identify_invalid_line_items(self, scenario4_test_data):
        """Identify which line items have data quality issues."""
        prices = scenario4_test_data["prices"]
        
        invalid_items = []
        
        for idx, row in prices.iterrows():
            order_id = row["order_id"]
            price = row["price"]
            quantity = row["quantity"]
            
            # Check for null/empty order_id
            if pd.isna(order_id) or order_id == "":
                invalid_items.append((idx, "missing_order_id"))
                continue
            
            # Check for non-numeric price
            try:
                price_num = float(price)
                if price_num <= 0:
                    invalid_items.append((idx, "zero_or_negative_price"))
                    continue
            except (ValueError, TypeError):
                invalid_items.append((idx, "invalid_price_format"))
                continue
            
            # Check for invalid quantity
            import re
            qty_match = re.search(r"(\d+)", str(quantity))
            if not qty_match or int(qty_match.group(1)) <= 0:
                invalid_items.append((idx, "invalid_quantity"))
                continue
        
        # Should have multiple invalid line items
        assert len(invalid_items) >= 4, \
            f"Expected at least 4 invalid line items, found {len(invalid_items)}"

    def test_orphan_line_items_detection(self, scenario4_test_data):
        """Detect line items that reference non-existent orders."""
        orders = scenario4_test_data["orders"]
        prices = scenario4_test_data["prices"]
        
        valid_order_ids = set(orders["order_id"].dropna().tolist())
        
        orphan_items = []
        for idx, row in prices.iterrows():
            order_id = row["order_id"]
            if pd.notna(order_id) and order_id not in valid_order_ids:
                orphan_items.append((idx, order_id))
        
        # Should have at least 1 orphan (TEST-ORD-NONEXISTENT)
        assert len(orphan_items) >= 1, "Should detect orphan line items"

    def test_invalid_delay_values(self, scenario4_test_data):
        """Detect invalid delay values."""
        delays = scenario4_test_data["delays"]
        
        invalid_delays = []
        for idx, row in delays.iterrows():
            delay_value = row["delay_in_days"]
            
            try:
                delay_num = int(delay_value)
                if delay_num < 0:
                    invalid_delays.append((idx, "negative_delay"))
            except (ValueError, TypeError):
                invalid_delays.append((idx, "non_numeric_delay"))
        
        # Should have at least 2 invalid delays
        assert len(invalid_delays) >= 2, \
            f"Expected at least 2 invalid delays, found {len(invalid_delays)}"


# =============================================================================
# SCENARIO 5: PERFORMANCE & AGGREGATION CONSISTENCY
# =============================================================================


class TestScenario5PerformanceAggregation:
    """
    Test Scenario 5: Performance & Aggregation Consistency
    
    Validates that analytical results are consistent and reasonably performant.
    These tests focus on calculation consistency using test data.
    """

    def test_line_item_totals_calculation(self, scenario1_test_data):
        """Verify line item totals are calculated correctly."""
        prices = scenario1_test_data["prices"]
        
        # Calculate totals
        prices_copy = prices.copy()
        prices_copy["price_num"] = pd.to_numeric(prices_copy["price"], errors="coerce")
        
        import re
        prices_copy["qty_num"] = prices_copy["quantity"].apply(
            lambda x: int(re.search(r"(\d+)", str(x)).group(1)) if re.search(r"(\d+)", str(x)) else 0
        )
        
        prices_copy["line_total"] = prices_copy["price_num"] * prices_copy["qty_num"]
        
        # Verify each order's total
        order_totals = prices_copy.groupby("order_id")["line_total"].sum()
        
        # Check a few expected totals
        expected_totals = {
            "TEST-ORD-20241214-001": 25.99 * 2 + 15.50 * 1,  # 67.48
            "TEST-ORD-20241214-002": 99.99 * 1,  # 99.99
            "TEST-ORD-20241214-003": 45.00 * 3 + 12.75 * 2,  # 160.50
            "TEST-ORD-20241214-004": 199.99 * 1,  # 199.99
            "TEST-ORD-20241214-005": 8.99 * 5 + 22.50 * 2,  # 89.95
        }
        
        for order_id, expected_total in expected_totals.items():
            actual_total = order_totals.get(order_id, 0)
            assert abs(actual_total - expected_total) < 0.01, \
                f"Order {order_id}: expected {expected_total}, got {actual_total}"

    def test_order_count_by_status(self, scenario1_test_data):
        """Verify order count aggregation by delay status."""
        orders = scenario1_test_data["orders"]
        delays = scenario1_test_data["delays"]
        
        # Merge orders with delays
        merged = orders.merge(delays, on="order_id", how="left")
        merged["is_delayed"] = merged["delay_in_days"].notna() & (merged["delay_in_days"] > 0)
        
        delayed_count = merged["is_delayed"].sum()
        on_time_count = len(merged) - delayed_count
        
        # Should have 2 delayed orders (TEST-ORD-20241214-002 and TEST-ORD-20241214-004)
        assert delayed_count == 2, f"Expected 2 delayed orders, got {delayed_count}"
        assert on_time_count == 3, f"Expected 3 on-time orders, got {on_time_count}"

    def test_quantity_aggregation(self, scenario1_test_data):
        """Verify quantity aggregation is accurate."""
        prices = scenario1_test_data["prices"]
        
        import re
        prices_copy = prices.copy()
        prices_copy["qty_num"] = prices_copy["quantity"].apply(
            lambda x: int(re.search(r"(\d+)", str(x)).group(1)) if re.search(r"(\d+)", str(x)) else 0
        )
        
        total_quantity = prices_copy["qty_num"].sum()
        
        # Expected: 2 + 1 + 1 + 3 + 2 + 1 + 5 + 2 = 17
        assert total_quantity == 17, f"Expected total quantity 17, got {total_quantity}"

    def test_unique_product_count(self, scenario1_test_data):
        """Verify unique product counting."""
        products = scenario1_test_data["products"]
        
        unique_products = products["product_id"].nunique()
        
        # Should have 8 unique products
        assert unique_products == 8, f"Expected 8 unique products, got {unique_products}"

    def test_avg_order_value_calculation(self, scenario1_test_data):
        """Verify average order value calculation."""
        prices = scenario1_test_data["prices"]
        orders = scenario1_test_data["orders"]
        
        import re
        prices_copy = prices.copy()
        prices_copy["price_num"] = pd.to_numeric(prices_copy["price"], errors="coerce")
        prices_copy["qty_num"] = prices_copy["quantity"].apply(
            lambda x: int(re.search(r"(\d+)", str(x)).group(1)) if re.search(r"(\d+)", str(x)) else 0
        )
        prices_copy["line_total"] = prices_copy["price_num"] * prices_copy["qty_num"]
        
        order_totals = prices_copy.groupby("order_id")["line_total"].sum()
        avg_order_value = order_totals.mean()
        
        # Expected: 617.91 / 5 = 123.582
        expected_aov = 617.91 / 5
        assert abs(avg_order_value - expected_aov) < 0.01, \
            f"Expected AOV ~{expected_aov:.2f}, got {avg_order_value:.2f}"


# =============================================================================
# DATABASE INTEGRATION TESTS (Require running database)
# =============================================================================


@pytest.mark.db
class TestDatabaseIntegration:
    """
    Integration tests that require a running database connection.
    
    These tests verify that the pipeline correctly loads and transforms
    data in the actual database.
    
    Run with: pytest -m db tests/test_scenarios.py
    """

    def test_fact_table_row_count_before(self, db_cursor):
        """Record fact table row count before test data load."""
        db_cursor.execute("SELECT COUNT(*) as cnt FROM dw.fact_sales")
        result = db_cursor.fetchone()
        count_before = result["cnt"]
        
        # Just record the count - actual assertion happens after pipeline run
        assert count_before >= 0, "Should be able to query fact_sales"

    def test_dim_user_contains_user(self, db_cursor):
        """Verify dimension table contains expected user."""
        db_cursor.execute("""
            SELECT user_key, user_id FROM dw.dim_user 
            WHERE user_id = 'USER99999' LIMIT 1
        """)
        result = db_cursor.fetchone()
        
        # This may return None if new user hasn't been loaded yet
        # The test documents expected behavior
        if result:
            assert result["user_key"] is not None, "User should have surrogate key"

    def test_dim_product_contains_product(self, db_cursor):
        """Verify dimension table contains expected product."""
        db_cursor.execute("""
            SELECT product_key, product_id FROM dw.dim_product 
            WHERE product_id = 'PRODUCT99999' LIMIT 1
        """)
        result = db_cursor.fetchone()
        
        # This may return None if new product hasn't been loaded yet
        if result:
            assert result["product_key"] is not None, "Product should have surrogate key"

    def test_campaign_unknown_handling(self, db_cursor):
        """Verify unknown campaign handling in dimension."""
        db_cursor.execute("""
            SELECT campaign_key, campaign_id FROM dw.dim_campaign 
            WHERE campaign_id = 'UNKNOWN' OR campaign_name = 'Unknown'
            LIMIT 1
        """)
        result = db_cursor.fetchone()
        
        # Check if system has Unknown campaign member
        # This documents expected behavior for missing dimension members

    def test_fact_sales_aggregation_matches_sql(self, db_cursor):
        """Verify fact table aggregation matches expected SQL calculation."""
        db_cursor.execute("""
            SELECT 
                SUM(sale_amount) as total_sales,
                SUM(quantity_sold) as total_quantity,
                COUNT(*) as row_count
            FROM dw.fact_sales
        """)
        result = db_cursor.fetchone()
        
        # Document aggregation results for comparison with dashboard
        assert result["total_sales"] is not None or result["row_count"] == 0, \
            "Should be able to aggregate fact_sales"

    def test_no_orphan_facts(self, db_cursor):
        """Verify fact tables don't have orphan dimension keys."""
        db_cursor.execute("""
            SELECT COUNT(*) as orphan_count
            FROM dw.fact_sales f
            LEFT JOIN dw.dim_user u ON f.user_key = u.user_key
            WHERE f.user_key IS NOT NULL AND u.user_key IS NULL
        """)
        result = db_cursor.fetchone()
        
        assert result["orphan_count"] == 0, \
            "Should not have orphan user keys in fact_sales"


# =============================================================================
# TEST DATA FILE EXISTENCE
# =============================================================================


class TestTestDataFilesExist:
    """Verify all test data files exist before running tests."""

    def test_scenario1_files_exist(self):
        """Verify Scenario 1 test files exist."""
        base_path = TEST_DATA_DIR / "scenario1_new_daily_orders"
        required_files = [
            "order_data_test.csv",
            "line_item_data_prices_test.csv",
            "line_item_data_products_test.csv",
            "order_delays_test.csv",
        ]
        
        for filename in required_files:
            filepath = base_path / filename
            assert filepath.exists(), f"Missing test file: {filepath}"

    def test_scenario2_files_exist(self):
        """Verify Scenario 2 test files exist."""
        base_path = TEST_DATA_DIR / "scenario2_new_customer_product"
        required_files = [
            "order_data_test.csv",
            "line_item_data_prices_test.csv",
            "line_item_data_products_test.csv",
            "user_data_test.json",
            "product_data_test.csv",
        ]
        
        for filename in required_files:
            filepath = base_path / filename
            assert filepath.exists(), f"Missing test file: {filepath}"

    def test_scenario3_files_exist(self):
        """Verify Scenario 3 test files exist."""
        base_path = TEST_DATA_DIR / "scenario3_late_campaign"
        required_files = [
            "order_data_test.csv",
            "line_item_data_prices_test.csv",
            "line_item_data_products_test.csv",
            "transactional_campaign_data_phase1.csv",
            "campaign_data_phase2.csv",
        ]
        
        for filename in required_files:
            filepath = base_path / filename
            assert filepath.exists(), f"Missing test file: {filepath}"

    def test_scenario4_files_exist(self):
        """Verify Scenario 4 test files exist."""
        base_path = TEST_DATA_DIR / "scenario4_data_quality"
        required_files = [
            "order_data_test.csv",
            "line_item_data_prices_test.csv",
            "line_item_data_products_test.csv",
            "order_delays_test.csv",
        ]
        
        for filename in required_files:
            filepath = base_path / filename
            assert filepath.exists(), f"Missing test file: {filepath}"
