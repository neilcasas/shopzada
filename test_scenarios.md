Scenario 1 — New Daily Orders File (End-to-End Pipeline Test)

Demonstrate that your system correctly handles an incremental batch load and updates the warehouse end-to-end.

Instructions:

Show the before state:
Fact table (or transaction table) row count for your main transaction fact (ex: fact_sales, transaction_table)
Dashboard KPI for the target date (ex: daily sales), before loading the test file
Run a small new orders CSV file through your full workflow.

Show the after state:

Fact table row count increased by the expected number of rows
Dashboard updated with correct totals for that date (ex: total sales = manual computation from the CSV)
In 2–3 sentences, explain how this confirms ingestion → transformation → load → analytics.

Upload:

An uninterrupted screen recording (video) of the whole process showing:

The contents of the test CSV file
The fact table (or transaction table) row counts BEFORE the upload
The metrics on the dashboard before the upload
The run of the workflow
The fact table (or transaction table) row counts AFTER the upload
The updated dashboard
A spoken explanation how this confirms ingestion → transformation → load → analytics

Scenario 2 — New Customer and New Product (Dimension Creation Test)

Demonstrate that your system correctly creates new dimension members and links them to a fact row.

Instructions:

Prepare test data for an order that uses a new customer and a new product that do not yet exist in your dimension tables (or entity tables).

Run your full workflow.

Show that:

The new customer appears in dim_customer or customer_table (or equivalent) with a valid surrogate key.

The new product appears in dim_product or product_table (or equivalent) with a valid surrogate key.

The corresponding fact row references those surrogate keys (no null or broken FKs).

The transaction appears correctly on the dashboard when filtered by that customer or product.

In 2–3 sentences, describe how your pipeline handles new dimension members (ex: key generation, upserts).

Upload:

An uninterrupted screen recording (video) of the whole process showing:

The contents of the test CSV file
The customer dimension table (or customer table) showing the new customer is NOT yet in the db.
The product dimension table (or product table) showing the new product is NOT yet in the db.
The run of the workflow including validations
The customer dimension (or customer table) table AFTER the upload showing new customer is in the db
The product dimension table (or product table) AFTER the upload showing new product is in the db
The updated dashboard showing the new transaction when filtered by the NEW customer
The updated dashboard showing the new transaction when filtered by the NEW product
Spoken description of how your pipeline handles new dimension members

Scenario 3 — Late & Missing Campaign Data (Unknown / Not Applicable Handling)

Demonstrate how your system handles optional or late-arriving data such as Campaign.

Instructions:

First, load orders that reference campaign codes which do not yet exist in your campaign dimension (or campaign table).

Show how those facts are handled:

Do they map to “Unknown” or “Not Applicable” campaign keys?

How are those cases represented in the dimension (table) and in the fact table (transactions)?

Next, load a campaign dimension file (or add a descriptor in the campaign table) that defines those previously missing campaigns.

Show the updated state of your campaign dimension (or campaign table) and explain what happens to new or existing facts.

In 3–4 sentences, explain your business rules for optional dimensions / tables (Unknown vs Not Applicable, late-arriving dimensions, etc.).

Upload:

An uninterrupted screen recording (video) of the whole process showing:

The contents of the test CSV files
The campaign dimension table (or campaign table) showing the contents BEFORE the 1st upload
The campaign dimension table (or campaign table) showing the contents AFTER the 1st upload
The affected fact rows (or transaction rows) AFTER the 1st upload
The dashboard view that includes the campaign breakdown after the 1st upload
The campaign dimension table (or campaign table) showing the contents AFTER the 2nd upload
The affected fact rows (or transaction rows) AFTER the 2nd upload
The dashboard view that includes the campaign breakdown after the 2nd upload
A spoken explanation of your business rules for optional dimensions / tables

Scenario 4 — Data Quality Failure & Error Handling

Demonstrate how your system behaves when encountering invalid or dirty data.

Instructions:

Create a test file that mixes valid and invalid records, example:

Invalid date format

Missing required IDs (customer, product, etc.)

Unknown foreign key values (ex: product ID not in the product dimension)

Run your ETL pipeline using this file.

Show that:

Valid rows are still successfully loaded into the fact table.

Invalid rows are either captured in a reject/error table or clearly logged as failures in your ETL logs.

The job does not silently corrupt data.

In a short reflection (4–5 sentences), describe:

Your data-quality rules

How you log or isolate bad data

How this design helps protect the integrity of the warehouse.

Upload:

An uninterrupted screen recording (video) of the whole process showing:

The contents of the test CSV file
The workflow run
The error logs on your orchestrator
The contents of your "reject" table (if applicable)
The fact table showing only valid rows are loaded
A spoken description of data quality rules, bad data logging/isolation, advantages if this to DWH's integrity

Scenario 5 — Performance & Aggregation Consistency

Demonstrate that your analytical layer is both consistent and reasonably performant.

Instructions:

Choose one analytical KPI or report (ex: monthly sales by channel, top N customers, product category sales).

In your BI tool (Tableau, Power BI, Metabase, etc.), show the KPI or chart.

Run an equivalent SQL query directly on your warehouse that computes the same aggregation.

Show that the totals or metrics match (or match within rounding differences).

Record how long the dashboard or query takes to run, and whether that is acceptable for your dataset size.

In 2–3 sentences, explain why cross-checking SQL vs dashboard is important for correctness.

Upload:

An uninterrupted screen recording (video) of the whole process showing:

The business intelligence dashboard view showing the target KPI
The SQL query execution
The SQL query results and the dashboard results side-by-side
A spoken explanation of why cross-checking SQL vs dashboard is important for correctness
