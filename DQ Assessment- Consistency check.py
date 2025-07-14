# Databricks notebook source
# DBTITLE 1,Check consistency function
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F
import uuid

def check_mismatch(master, reference, column_name, batch_id):
    """
    Checks for mismatches between the master and reference tables based on a column.

    Args:
        master (str): Name of the master table.
        reference (str): Name of the reference table.
        column_name (str): The name of the column to check.
        batch_id (str): Unique batch ID for the function call.

    Returns:
        DataFrame: A DataFrame containing mismatch metrics with their observations.
    """

    # Prepare DataFrames
    master_df = spark.table(master).alias("master")
    reference_df = spark.table(reference).alias("reference")

    # Find rows in master not in reference
    master_not_in_reference = master_df.join(
        reference_df,
        F.col(f"master.{column_name}") == F.col(f"reference.{column_name}"),
        how="left"
    ).filter(F.col(f"reference.{column_name}").isNull()).select(F.col(f"master.{column_name}")).distinct()

    # Count mismatches for master not in reference
    master_not_in_reference_count = master_not_in_reference.count()
    total_records_master = master_df.count()
    mismatch_percentage_master = (master_not_in_reference_count / total_records_master) * 100 if total_records_master > 0 else 0

    # Find rows in reference not in master
    reference_not_in_master = reference_df.join(
        master_df,
        F.col(f"reference.{column_name}") == F.col(f"master.{column_name}"),
        how="left"
    ).filter(F.col(f"master.{column_name}").isNull()).select(F.col(f"reference.{column_name}")).distinct()

    # Count mismatches for reference not in master
    reference_not_in_master_count = reference_not_in_master.count()
    total_records_reference = reference_df.count()
    mismatch_percentage_reference = (reference_not_in_master_count / total_records_reference) * 100 if total_records_reference > 0 else 0

    # Calculate cardinality for the master and reference tables
    master_cardinality = master_df.select(column_name).distinct().count()
    reference_cardinality = reference_df.select(column_name).distinct().count()

    # Duplicates check on master data
    master_duplicates_count = master_df.groupBy(column_name).agg(
        F.count("*").alias("count")
    ).filter(F.col("count") > 1).agg(F.sum("count") - F.count("count")).collect()[0][0]

    # Handle None case for master_duplicates_count
    master_duplicates_count = master_duplicates_count if master_duplicates_count is not None else 0


    # Null value checks
    null_count_master = master_df.filter(F.col(column_name).isNull()).count()
    null_count_reference = reference_df.filter(F.col(column_name).isNull()).count()
    percentage_null_master = (null_count_master / total_records_master) * 100 if total_records_master > 0 else 0
    percentage_null_reference = (null_count_reference / total_records_reference) * 100 if total_records_reference > 0 else 0

    # Define schema for insights
    insights_schema = StructType([
        StructField("Table ID", StringType(), True),
        StructField("Column ID", StringType(), True),
        StructField("Metric Name", StringType(), True),
        StructField("Master Table Value", StringType(), True),
        StructField("Reference Table Value", StringType(), True),
        StructField("Observation", StringType(), True)
    ])
    
    # qualified_column_name =  f"{reference}.{column_name}"

    # Prepare insights data
    insights_data = [

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()),"Metric Name": "Table name",
         "Master Table Value": str(master),
         "Reference Table Value": str(reference),
         "Observation": "Table name"},
        
        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()),"Metric Name": "Column name",
         "Master Table Value": str(column_name),
         "Reference Table Value": str(column_name),
         "Observation": "Master and reference table key column name matches"},
        
        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Cardinality",
         "Master Table Value": str(master_cardinality),
         "Reference Table Value": str(reference_cardinality),
         "Observation": "Consistent" if master_cardinality == reference_cardinality else "Inconsistent"},

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Duplicates Count",
         "Master Table Value": str(master_duplicates_count),
         "Reference Table Value": "N/A",
         "Observation": "High number of duplicates in master table" if master_duplicates_count > 0 else "No duplicates in master table"},

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Null Count",
         "Master Table Value": str(null_count_master),
         "Reference Table Value": str(null_count_reference),
         "Observation": "High proportion of null values" if null_count_master > 0 or null_count_reference > 0 else "No null values"},

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Percentage of Null Values",
         "Master Table Value": f"{percentage_null_master:.2f}%",
         "Reference Table Value": f"{percentage_null_reference:.2f}%",
         "Observation": "Higher percentage of nulls in master" if percentage_null_master > percentage_null_reference else \
                        "Higher percentage of nulls in reference" if percentage_null_master < percentage_null_reference else "Equal percentage of nulls"},

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Master rows Not in Reference",
         "Master Table Value": str(master_not_in_reference_count),
         "Reference Table Value": "N/A",
         "Observation": f"{mismatch_percentage_master:.2f}% rows in master not found in reference"},

        {"Table ID": batch_id, "Column ID": str(uuid.uuid4()), "Metric Name": "Reference rows Not in Master",
         "Master Table Value": "N/A",
         "Reference Table Value": str(reference_not_in_master_count),
         "Observation": f"{mismatch_percentage_reference:.2f}% rows in reference not found in master"}
    ]

    # Create DataFrame using schema
    insights = spark.createDataFrame(insights_data, schema=insights_schema)

    return master_not_in_reference, reference_not_in_master, insights


# COMMAND ----------

# DBTITLE 1,pivot
from pyspark.sql.functions import col, concat, lit, instr, substring

def pivot_consistency_metrics(df):
    """
    Converts rows to columns using pivot, grouping by Table ID and Column ID.

    Args:
        df: Input DataFrame with schema (Table ID, Column ID, Metric Name, Master Table Value, Reference Table Value, Observation).

    Returns:
        A DataFrame with pivoted columns for Metric Name, Master Table Value, Reference Table Value, and Observation.
    """
    # Combine Master Table Value, Reference Table Value, and Observation for pivoting
    combined_df = df.withColumn(
        "Metric Combined", 
        concat(
            col("Master Table Value").cast("string"), 
            lit(" | "), 
            col("Reference Table Value").cast("string"), 
            lit(" | "), 
            col("Observation").cast("string")
        )
    )

    # Perform pivot on "Metric Name" with combined values
    pivoted_df = combined_df.groupBy("Table ID", "Column ID") \
        .pivot("Metric Name") \
        .agg({"Metric Combined": "first"})  # Aggregate with 'first' (to handle duplicate metrics)

    # Split combined columns back into Master Table Value, Reference Table Value, and Observation
    for metric_name in pivoted_df.columns[2:]:  # Skip Table ID and Column ID
        pivoted_df = pivoted_df.withColumn(
            metric_name + " Master Table Value", 
            substring(col(metric_name), 1, instr(col(metric_name), " | ") - 1)  # Extract Master Table Value
        ).withColumn(
            metric_name + " Reference Table Value",
            substring(
                col(metric_name), 
                instr(col(metric_name), " | ") + 3, 
                instr(substring(col(metric_name), instr(col(metric_name), " | ") + 3, 1000), " | ") - 1
            )  # Extract Reference Table Value
        ).withColumn(
            metric_name + " Observation",
            substring(
                col(metric_name), 
                instr(substring(col(metric_name), instr(col(metric_name), " | ") + 3, 1000), " | ") + instr(col(metric_name), " | ") + 2, 
                1000
            )  # Extract Observation
        ).drop(metric_name)

    return pivoted_df


# COMMAND ----------

# DBTITLE 1,call consistency function
# Define parameters for function calls
parameters = [
#     {"master": "beliv_prd.catalogs.vw_client_latest", "reference": "beliv_prd.commercial.facturacion_explosionada", "column_name": "client_code_custom"},
#     {"master": "beliv_prd.catalogs.vw_client_latest", "reference": "beliv_prd.commercial.facturacion_agrupada", "column_name": "client_code_custom"},
#     {"master": "beliv_prd.catalogs.vw_client_latest", "reference": "beliv_prd.commercial.facturacion_agrupada", "column_name": "client_code"},
#     {"master": "beliv_prd.catalogs.vw_country_beliv_latest", "reference": "beliv_prd.commercial.facturacion_agrupada", "column_name": "country_code"}

# {"master": "beliv_prd.catalogs.organization_beliv", "reference": "beliv_prd.commercial.theoretical_inventory", "column_name": "organization_code"},
# {"master": "beliv_prd.catalogs.location_beliv", "reference": "beliv_prd.commercial.theoretical_inventory", "column_name": "location_code"},
{"master": "beliv_prd.catalogs.vw_sku_beliv_latest", "reference": "beliv_prd.commercial.theoretical_inventory", "column_name": "sku_code"}
# {"master": "beliv_prd.catalogs.tipo_cambio", "reference": "beliv_prd.commercial.theoretical_invefntory", "column_name": "currency_code"}
]

# Initialize an empty DataFrame to hold all insights
final_consistency_insights_df = None

# Iterate through parameters and make function calls
for param in parameters:
    batch_id = str(uuid.uuid4())  # Generate a unique batch ID
    _, _, insights = check_mismatch(param["master"], param["reference"], param["column_name"], batch_id)

    # Append results to the final DataFrame
    if final_consistency_insights_df is None:
        final_consistency_insights_df = insights
    else:
        final_consistency_insights_df = final_consistency_insights_df.union(insights)

# Display the final insights DataFrame
# final_consistency_insights_df1q2  .show(truncate=False)
display(final_consistency_insights_df)

# COMMAND ----------

def join_and_filter_outliers(outlier_df, final_insights_df,filter_metric_names):
    """
    Joins the outlier DataFrame with the final insights DataFrame and filters based on specific conditions.

    Args:
        outlier_df: The DataFrame prepared for outlier metrics.
        final_insights_df: The input DataFrame containing metrics.

    Returns:
        A DataFrame containing joined and filtered results with all columns from the `columns_df`.
    """
    # Alias the columns DataFrame for clarity
    columns_df = final_insights_df.alias("columns")
    
    # Perform join and filtering
    return outlier_df.join(
        columns_df,
        on=outlier_df["Column ID"] == columns_df["Column ID"],  # Explicit join condition to avoid ambiguity
        how="inner"
    ).filter(
        # columns_df["Metric Name"].isin("Percentage of Outlier Records", "Column name")  # Use `isin` for clarity
        columns_df["Metric Name"].isin(*filter_metric_names)  # Use `isin` for clarity
    ).select(
        *[f"columns.{col}" for col in columns_df.columns]  # Select all columns from `columns_df`, qualified with the alias
    )


# COMMAND ----------

from pyspark.sql.functions import col

filter_metric_names = ["Table name"]
# Step 2: Perform the join and filter
result_df1 = join_and_filter_outliers(final_consistency_insights_df, final_consistency_insights_df, filter_metric_names).select("Table ID", "Master Table Value", "Reference Table Value")

# Rename duplicate columns in `final_consistency_insights_df`, excluding the join key (`Table ID`)
renamed_final_consistency_insights_df = final_consistency_insights_df.select(
    [col(c).alias(c + "_dup") if c != "Table ID" and c in result_df1.columns else col(c) for c in final_consistency_insights_df.columns]
)

# Perform the join
result_df2 = result_df1.join(
    renamed_final_consistency_insights_df,
    on="Table ID",
    how="inner"
)

# Display the result
# Correct usage of isin() for filtering
display(result_df2.filter(col("Metric Name").isin("Reference rows Not in Master", "Master rows Not in Reference")).select([c for c in result_df2.columns if c not in ["Table ID", "Column ID"]]))



# COMMAND ----------


# final_consistency_pivoted_df = pivot_consistency_metrics(result_df2.distinct())
# display(final_consistency_pivoted_df)
# display(final_consistency_insights_df.distinct())

# COMMAND ----------


# Read the master table from Unity Catalog
master_table = spark.sql("SELECT * FROM beliv_prd.catalogs.location_beliv")

# Read the reference table from Unity Catalog
reference_table = spark.sql("SELECT * FROM beliv_prd.commercial.theoretical_inventory")

# Perform the anti-join to find rows in master_table not in reference_table
missing_rows = master_table.join(
    reference_table,
    master_table.location_code == reference_table.location_code,
    "left_anti"
)

# Count the missing rows
missing_count = missing_rows.select("location_code").distinct().count()

# Print the count
print(f"Number of rows in the master table but missing in the reference table: {missing_count}")

# (Optional) Display the missing rows for verification
# missing_rows.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Read the master table from Unity Catalog
master_df = spark.sql("SELECT * FROM beliv_prd.catalogs.location_beliv")

# Read the reference table from Unity Catalog
reference_df = spark.sql("SELECT * FROM beliv_prd.commercial.theoretical_inventory")

column_name = "location_code"

reference_not_in_master = reference_df.join(
        master_df,
        F.col(f"reference_df.{column_name}") == F.col(f"master_df.{column_name}"),
        how="left"
    ).filter(F.col(f"master_df.{column_name}").isNull()).select(F.col(f"reference_df.{column_name}")).distinct()

reference_not_in_master.count()


# COMMAND ----------

from pyspark.sql import functions as F

# Read the master table from Unity Catalog
reference_df= spark.sql("SELECT * FROM beliv_prd.catalogs.location_beliv")

# Read the reference table from Unity Catalog
master_df  = spark.sql("SELECT * FROM beliv_prd.commercial.theoretical_inventory")

column_name = "location_code"

# Perform the join
reference_not_in_master = reference_df.alias("ref").join(
    master_df.alias("mast"),
    F.col(f"ref.{column_name}") == F.col(f"mast.{column_name}"),
    how="left"
).filter(
    F.col(f"mast.{column_name}").isNull()
).select(
    F.col(f"ref.{column_name}")
).distinct()

# Count the results
result_count = reference_not_in_master.count()
print(f"Number of reference records not in master: {result_count}")
