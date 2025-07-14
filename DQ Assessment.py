# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType,StructType, StructField
import pandas as pd
import uuid


# COMMAND ----------

# DBTITLE 1,Main function to do Data Profiling

def calculate_null_percentage_outliers_and_rare_categories_spark(table_name, zzcolumns, num_list, cat_list,text_list,table_id, rare_threshold=0.01):

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, NumericType,StructType, StructField
    import pandas as pd
    import uuid

    columns = num_list + cat_list + text_list
    # columns = text_list

       # Define schema for insights
    insights_schema = StructType([
        StructField("Table ID", StringType(), True),
        StructField("Column ID", StringType(), True),
        StructField("Metric ID", StringType(), True),
        StructField("Metric Name", StringType(), True),
        StructField("Metric Value", StringType(), True),
        StructField("Observation", StringType(), True)
    ])

    # Retrieve the Spark DataFrame based on table_name
    dfspark = spark.table(table_name).sample(withReplacement=False, fraction=0.01, seed=42)

    # Check if provided columns exist in the DataFrame
    for column in columns:
        if column not in dfspark.columns:
            raise ValueError(f"Column {column} not found in the table {table_name}.")

    
    # List of columns to exclude
    exclude_cols = ['ingest_date', 'report_date']

    # Select columns to include in the duplicate check
    columns_to_check = [col for col in dfspark.columns if col not in exclude_cols]

    # # Group by the selected columns and count occurrences
    # duplicates_df = dfspark.groupBy(*columns_to_check).count()

    # # Filter to get only duplicate rows (count > 1)
    # duplicates_only_df = duplicates_df.filter(F.col("count") > 1)

    # # Count the number of duplicate rows
    # duplicate_row_count = duplicates_only_df.agg(F.sum("count") - F.count("*")).first()[0]

    # # Show the duplicates
    # # duplicates_only_df.show()

    # print(f"Table name  -  {table_name}")
    # print(f"Number of duplicate rows (excluding columns {exclude_cols}): {duplicate_row_count}")

    # Create an empty list to store results
    results = []
    
    # Initialize the insights data as a list of dictionaries
    insights_data = []

    # Loop through each column and calculate null percentages, outliers, and rare categories
    for column in columns:

        unique_column_id = str(uuid.uuid4())

        percentile_value = None
        filtered_count = None
        percentage_greater = None
        data_quality = None
        percentile = None
        frequency_threshold = None
        percentile_formatted = None
        frequency_threshold_formatted = None
        outliers_list = []
        outliers_count = None
        percentage_of_outliers =None


        # Get the column's data type
        column_type = dict(dfspark.dtypes)[column]
        
        # Calculate null count and total count
        null_count = dfspark.filter(dfspark[column].isNull()).count()
        total_count = dfspark.count()
        
        # Calculate distinct values count
        distinct_count = dfspark.select(column).distinct().count()

        # Calculate null percentage
        null_percent = round((null_count / total_count) * 100, 3) if total_count > 0 else 0

        # Calculate distinct values percentage
        dist_values_percent = round((distinct_count / total_count) * 100, 3) if total_count > 0 else 0

        # Initialize outliers list and rare categories list to empty
        outliers = []
        rare_categories = []
        outliers_count = None

        # Only calculate outliers for numeric columns
        # if isinstance(dfspark.schema[column].dataType, NumericType):


        if column in num_list:
            try:
                percentile = 0.99
                percentile_formatted = percentile * 100

                # Ensure the column exists and has valid data
                if column not in dfspark.columns:
                    print(f"Warning: Column '{column}' not found in the DataFrame. Skipping...")
                    continue  # Skip to the next column

                # Calculate the specified percentile
                stats = dfspark.approxQuantile(column, [percentile], 0.01)
                if not stats or len(stats) == 0:
                    print(f"Warning: Unable to calculate percentile for column '{column}'. Skipping...")
                    continue  # Skip to the next column

                # Extract the specified percentile value
                percentile_value = stats[0]

                # Total number of records in the DataFrame
                total_records = dfspark.count()
                if total_records == 0:
                    print(f"Warning: DataFrame is empty. Skipping column '{column}'...")
                    continue  # Skip to the next column

                zero_count = dfspark.filter(dfspark[column] == 0).count()
                negative_count = dfspark.filter(dfspark[column] < 0).count()

                # Calculate the percentages
                zero_percentage = (zero_count / total_records) * 100 if total_records > 0 else 0
                negative_percentage = (negative_count / total_records) * 100 if total_records > 0 else 0


                # Filter rows greater than the specified percentile
                filtered_df = dfspark.filter(dfspark[column] > percentile_value)
                filtered_count = filtered_df.count()  # Count records greater than the percentile

                # Calculate the percentage of records greater than the specified percentile
                percentage_greater = (filtered_count / total_records) * 100

                # Classify data quality based on the percentage of outliers
                if percentage_greater <= 1:
                    data_quality = "High"
                elif 1 < percentage_greater <= 5:
                    data_quality = "Moderate"
                elif percentage_greater > 5:
                    data_quality = "Low"
                else:
                    data_quality = None

                # Print the results (or save them as needed)
                print(f"Column: {column}, Percentile Value: {percentile_value}, Percentage Greater: {percentage_greater:.2f}%, Data Quality: {data_quality}")

            except Exception as e:
                # Log the error and continue with the next column
                print(f"Error processing column '{column}': {str(e)}")
                continue

    
        if column in cat_list or column in text_list:
        # if column in text_list:
            frequency_threshold = 0.01
            frequency_threshold_formatted = frequency_threshold * 100

                # Calculate the frequency of each category in the column
            category_counts = dfspark.groupBy(column).count().withColumn(
                "frequency", F.col("count") / total_count
            )
            
            # Filter categories with frequency below the threshold (these are considered outliers)
            outliers_df = category_counts.filter(F.col("frequency") < frequency_threshold)
            
            # Collect the outliegr categories
            outliers = [row[column] for row in outliers_df.collect()]
            outliers_count = outliers_df.count()  # Number of outlier categories
            
            # Calculate the percentage of records with outlier categories
            total_outlier_records = dfspark.filter(F.col(column).isin(outliers)).count()
            percentage_of_outliers = (total_outlier_records / total_count) * 100
            
            # Return the outlier categories DataFrame, the count of outliers, and the percentage of outliers
            # return outliers_df, outliers_count, percentage_of_outliers

            if percentage_of_outliers < 1:
                data_quality = "High"
            elif 1 < percentage_of_outliers <= 5:
                data_quality = "Moderate"
            elif percentage_of_outliers > 5:
                data_quality = "Low"
            else:
                data_quality = None

            # outliers_list = outliers_df.collect()  # Collect rows as a list
            # print(f"Outlier Categories: {[row[column] for row in outliers_list]}")  
            # print(f"Number of Outlier Categories: {outliers_count}")
            # print(f"Percentage of Records with Outliers: {percentage_of_outliers:.2f}%")
            # print(f"Data Quality Classification: {data_quality}")

        # Append the result as a tuple (table_name, column_name, null_percentage, outliers, rare_categories)
        if percentage_greater is not None:
            rounded_percentage_greater = round(percentage_greater, 3)
        else:
            rounded_percentage_greater = None  # Handle the None case appropriately

        if percentage_of_outliers is not None:
            rounded_percentage_of_outliers = round(percentage_of_outliers, 3)
        else:
            rounded_percentage_of_outliers = None 

        qualified_column_name =  f"{table_name}.{column}"

                # Add the insights as a dictionary to the list
        # insights_data.append({
        #     "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
        #     "Metric Name": "Table name",
        #     "Metric Value": str(table_name),
        #     "Observation": "Table name"
        # })
        # insights_data.append({
        #     "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
        #     "Metric Name": "Column name",
        #     "Metric Value": str(column),
        #     "Observation": f"Metrics for column '{column}'"
        # })
        insights_data.append({ 
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
            "Metric Name": "Column name",
            "Metric Value": str(qualified_column_name),
            "Observation": "Table and column information"
        })
        insights_data.append({ 
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
            "Metric Name": "Data Type",
            "Metric Value": str(column_type),
            "Observation": "Column Data Type Information"
        })
        insights_data.append({
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()),           
            "Metric Name": "Total Count",
            "Metric Value": str(total_count),
            "Observation": f"Total rows in the table."
        })
        insights_data.append({
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()),           
            "Metric Name": "Cardinality",
            "Metric Value": str(distinct_count),
            "Observation": f"Column '{column}' has {dist_values_percent}% distinct values."
        })

        insights_data.append({
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()),           
            "Metric Name": "Distinct Values Percentage",
            "Metric Value": str(dist_values_percent),
            "Observation": f"Column '{column}' has {dist_values_percent}% distinct values."
        })
         
        insights_data.append({
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()),           
            "Metric Name": "Null Percentage",
            "Metric Value": f"{null_percent}",
            "Observation": f"Column '{column}' has {null_percent}% null values."
        })

        if column in num_list:
            
            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Zero Count",
                "Metric Value": str(zero_count),
                "Observation": f"Zero count for column '{column}' is {zero_count}."
            })

            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Negative Count",
                "Metric Value": str(negative_count),
                "Observation": f"Negative count for column '{column}' is {negative_count}."
            })

            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Zero Count Percentage",
                "Metric Value": str(round(zero_percentage,3)),
                "Observation": f"Zero Count: {zero_count} ({zero_percentage:.2f}%)"
            })

            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Negative Count Percentage",
                "Metric Value": str(round(negative_percentage,3)),
                "Observation": f"Negative Count: {negative_count} ({negative_percentage:.2f}%)"
            })
            
            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "99th Percentile Value",
                "Metric Value": str(percentile_value),
                "Observation": f"99th percentile value for column '{column}' is {percentile_value}."
            })
            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Percentage Greater than 99th Percentile",
                "Metric Value": f"{rounded_percentage_greater}",
                "Observation": f"{rounded_percentage_greater}% of values in column '{column}' are above the 99th percentile."
            })

        if column in cat_list or column in text_list:
        # if column in text_list:

            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Rare Categories Count",
                "Metric Value": str(outliers_count),
                "Observation": f"Column '{column}' has {outliers_count} rare categories."
            })
            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Percentage of Outlier Records",
                "Metric Value": f"{rounded_percentage_of_outliers}",
                "Observation": f"{rounded_percentage_of_outliers}% of records in column '{column}' belong to rare categories."
            })
            insights_data.append({
                "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
                "Metric Name": "Outlier Values",
                "Metric Value": f"{outliers}",
                "Observation": f"outlier values for column '{column}' are {outliers}"
            })

        insights_data.append({
            "Table ID": str(table_id), "Column ID": str(unique_column_id), "Metric ID": str(uuid.uuid4()), 
            "Metric Name": "Data Quality Classification",
            "Metric Value": data_quality,
            "Observation": f"Overall data quality for column '{column}' is {data_quality}."
        })


    # Create DataFrame using schema
    insights = spark.createDataFrame(insights_data, schema=insights_schema)

    return insights


# COMMAND ----------

# DBTITLE 1,create parameters
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

# List of tables
tables = [
# "beliv_prd.commercial.nocbc_sales",
# "beliv_prd.commercial.nocbc_inventories",
# "beliv_prd.catalogs.modelos_negocio",
# "beliv_prd.financial.partidas_individuales",
"beliv_prd.catalogs.tipo_cambio"
# "beliv_prd.catalogs.conversion_factor_beliv",
# "beliv_prd.commercial.theoretical_inventory",
# "beliv_prd.commercial.cbc_inventory"
# "beliv_prd.catalogs.organization_beliv",
# "beliv_prd.catalogs.location_beliv",
# "beliv_prd.catalogs.warehouse_beliv",
# "beliv_prd.commercial.facturacion_agrupada",
# "beliv_prd.commercial.facturacion_explosionada"
# "beliv_prd.catalogs.sku_equivalence_matrix",
# "beliv_prd.catalogs.client_equivalence_matrix"
]

# Initialize an empty list to store the results
results = []

# Loop through each table
for table in tables:
    # Read the table into a Spark DataFrame
    df = spark.table(table)
    
    # Initialize empty lists to store column names based on type
    categorical_cols = []
    numerical_cols = []
    text_cols = []
    
    # Analyze columns using schema
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        # Identify numerical columns (e.g., IntegerType, DoubleType)
        if isinstance(col_type, NumericType):
            numerical_cols.append(col_name)
        # Identify string columns and categorize as categorical or text
        elif isinstance(col_type, StringType):
            # Categorical columns: fewer unique values than a threshold
            unique_values_count = df.select(F.col(col_name)).distinct().count()
            if unique_values_count < 20:
                categorical_cols.append(col_name)
            else:
                text_cols.append(col_name)
    
    # Append the results
    results.append((table, numerical_cols, categorical_cols, text_cols))

# Create a DataFrame from the results
schema = ["table_name", "numerical_columns", "categorical_columns", "text_columns"]
result_df = spark.createDataFrame(results, schema)

# Display the result DataFrame
display(result_df)


# COMMAND ----------

# DBTITLE 1,Call Data Profiling function
final_insights_df = None
parameters_df = result_df

limited_parameters = parameters_df #.limit(3)

for row in limited_parameters.collect():
    param = row.asDict()  # Convert Row object to dictionary
    batch_id = str(uuid.uuid4())  # Generate a unique batch ID
    # Print the function parameters
    print("Function Parameters:")
    print(f"table_name: {param['table_name']}")
    print(f"numerical_columns: {param['numerical_columns']}")
    print(f"categorical_columns: {param['categorical_columns']}")
    print(f"text_columns: {param['text_columns']}")
    print(f"batch_id: {batch_id}")
    print(f"rare_threshold: {0.005}")
    insights = calculate_null_percentage_outliers_and_rare_categories_spark(param["table_name"], [], param["numerical_columns"], param["categorical_columns"]+param["text_columns"], [] ,batch_id, 0.005)

    # Append results to the final DataFrame
    if final_insights_df is None:
        final_insights_df = insights
    else:
        final_insights_df = final_insights_df.union(insights)

# Display the final insights DataFrame
display(final_insights_df)

# COMMAND ----------

# final_insights_df = calculate_null_percentage_outliers_and_rare_categories_spark("beliv_prd.commercial.facturacion_explosionada", [],  ['refused_code', 'cases', 'equivalent_units', 'cases_invoiced', 'units_invoiced', 'cases_logistic', 'units_logistic', 'sales_with_taxes', 'gross_sales', 'public_price', 'year', 'month', 'sales_doc_code', 'year_quarter'], ['invoice_code', 'client_code', 'sku_code', 'combo_code', 'sales_office', 'commercial_region_code', 'location_code', 'payment_terms_key', 'invoice_type', 'cancellation_doc_ref_code', 'combo_code_custom', 'client_code_custom', 'sku_code_custom','country_code', 'organization_code', 'sector_code', 'sales_organization_code', 'currency_commercial_document', 'currency_document_code', 'royalty', 'is_combo', 'distribution_channel_code', 'sales_units_of_measure', 'source', 'commercial_region_name'], [],1000, 0.005)


# COMMAND ----------

# DBTITLE 1,Distinct Metric Names
distinct_metric_names_df = final_insights_df.select("Metric Name").distinct()
display(distinct_metric_names_df)

# COMMAND ----------

# DBTITLE 1,Top Tables with High Null Percentage
from pyspark.sql.functions import regexp_replace

# Identify columns with Null Percentage greater than 0
null_insights = final_insights_df.filter(final_insights_df["Metric Name"] == "Null Percentage") \
    # .filter(regexp_replace(final_insights_df["Metric Value"], "%", "").cast("double") > 0) \
    # .select("Table ID", "Column ID", "Metric Value", "Observation")

display(null_insights)


# COMMAND ----------

# DBTITLE 1,Get table and column context
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

# DBTITLE 1,Pivot table
from pyspark.sql.functions import col, concat, lit, instr, substring

def pivot_metrics(df):
    """
    Converts rows to columns using pivot, grouping by Table ID and Column ID.

    Args:
        df: Input DataFrame with schema (Table ID, Column ID, Metric Name, Metric Value, Observation).

    Returns:
        A DataFrame with pivoted columns for Metric Name, Metric Value, and Observation.
    """
    # Combine Metric Value and Observation for pivoting
    combined_df = df.withColumn(
        "Metric Combined", 
        concat(
            col("Metric Value").cast("string"), 
            lit(": "), 
            col("Observation").cast("string")
        )
    )

    # Perform pivot on "Metric Name" with combined values
    pivoted_df = combined_df.groupBy("Table ID", "Column ID") \
        .pivot("Metric Name") \
        .agg({"Metric Combined": "first"})  # Aggregate with 'first' (to handle duplicate metrics)

    # Split combined columns back into Metric Value and Observation
    for metric_name in pivoted_df.columns[2:]:  # Skip Table ID and Column ID
        pivoted_df = pivoted_df.withColumn(
            metric_name + " Value", 
            substring(col(metric_name), 1, instr(col(metric_name), ":") - 1)  # Extract Metric Value
        ).withColumn(
            metric_name + " Observation",
            substring(col(metric_name), instr(col(metric_name), ":") + 2, 1000)  # Extract Observation
        ).drop(metric_name)

    return pivoted_df


# COMMAND ----------

# DBTITLE 1,Table: cbc_inventory
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%tipo_cambio%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
#  "Outlier Values Value",
#  "Data Quality Classification Value"
 )
)

# COMMAND ----------

table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%facturacion_explosionada%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
#  "Outlier Values Value",
#  "Data Quality Classification Value"
 )
)

# COMMAND ----------

# DBTITLE 1,Outlier information
# Prepare the outlier data with numeric values
outlier_df = final_insights_df.filter(final_insights_df["Metric Name"] == "Percentage of Outlier Records") \
    .withColumn("Numeric Metric Value", regexp_replace(final_insights_df["Metric Value"], "%", "").cast("double")) \
    .alias("outliers")

# Alias the `final_insights_df` as `columns` for the join
# columns_df = final_insights_df.alias("columns")


# COMMAND ----------

# DBTITLE 1,Function call to get context

filter_metric_names = ["Percentage of Outlier Records","Outlier Values", "Column name"]

# Step 2: Perform the join and filter
result_df1 = join_and_filter_outliers(outlier_df, final_insights_df,filter_metric_names)

# display(result_df1.distinct().orderBy("Table ID", "Column ID", "Metric Name", ascending=False))
# display(result_df1.distinct().orderBy("Column ID", "Metric Name"))

pivoted_df = pivot_metrics(result_df1.distinct())
display(pivoted_df)

# COMMAND ----------

# DBTITLE 1,Null Insights
from pyspark.sql.functions import regexp_replace

# Identify columns with Null Percentage greater than 0
null_insights = final_insights_df.filter(final_insights_df["Metric Name"] == "Null Percentage") \
    # .filter(regexp_replace(final_insights_df["Metric Value"], "%", "").cast("double") > 0) \
    # .select("Table ID", "Column ID", "Metric Value", "Observation")

# display(null_insights)


# COMMAND ----------

# DBTITLE 1,Get Null Insights context

filter_metric_names = ["Null Percentage", "Column name"]

# Step 2: Perform the join and filter
result_df1 = join_and_filter_outliers(outlier_df, final_insights_df,filter_metric_names)

pivoted_df = pivot_metrics(result_df1.distinct())
display(pivoted_df)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Table : conversion_factor_beliv
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%conversion_factor_beliv%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
#  "Rare Categories Count Value",
#  "Percentage of Outlier Records Value",
#  "Outlier Values Value",
 "Data Quality Classification Value"
 )
)

# COMMAND ----------

# DBTITLE 1,Table : theoretical_inventory
df_ti = spark.table("beliv_prd.commercial.theoretical_inventory")
# df_ti.filter( df_ti["currency_code"] == None).count()
display(df_ti.filter(df_ti["currency_code"].isNull()).select("currency_code"))


# COMMAND ----------

# DBTITLE 1,Table : theoretical_inventory
duplicate_count = df_ti.groupBy(df_ti.columns).count().filter("count > 1").count()
duplicate_count

# COMMAND ----------

# DBTITLE 1,Table : theoretical_inventory
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%theoretical_inventory%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
 "Outlier Values Value",
 "Data Quality Classification Value"
 )
)

# COMMAND ----------

# DBTITLE 1,Table : location_beliv
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%location_beliv%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
 "Outlier Values Value",
 "Data Quality Classification Value"
 )
)

# COMMAND ----------

# DBTITLE 1,Table : location_beliv
df_location = spark.table("beliv_prd.catalogs.location_beliv")
# display(df_ti.filter(df_ti["currency_code"].isNull()).select("currency_code"))
duplicate_count = df_location.groupBy(df_location.columns).count().filter("count > 1").count()
duplicate_count

# COMMAND ----------

# DBTITLE 1,Table : warehouse_beliv
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%warehouse_beliv%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
 "Outlier Values Value",
 "Data Quality Classification Value"
 )
)

# COMMAND ----------

# DBTITLE 1,Table : warehouse_beliv
df_warehouse = spark.table("beliv_prd.catalogs.warehouse_beliv")
distinct_codes = df_warehouse.select("location_code", "warehouse_code").distinct().groupBy("location_code").agg(F.count("warehouse_code").alias("distLocCount"))
display(distinct_codes)

# COMMAND ----------

# DBTITLE 1,beliv_prd.catalogs.organization_beliv
table_id = final_insights_df.filter(final_insights_df["Metric Value"].like("%organization_beliv%")).select("Table ID").distinct().collect()[0][0]
table_entries_all = final_insights_df.filter(final_insights_df["Table ID"] == table_id)
# display(table_entries_all)
pivoted_df = pivot_metrics(table_entries_all.distinct())
display(pivoted_df.select(
 "Table ID",
 "Column ID",
 "Column name Value",
 "Data Type Value",
 "Total Count Value",
 "Cardinality Value",
 "Zero Count Value",
 "Negative Count Value",
 "Zero Count Percentage Value",
 "Negative Count Percentage Value",
 "Distinct Values Percentage Value",
 "Null Percentage Value",
 "99th Percentile Value Value",
 "Percentage Greater than 99th Percentile Value",
 "Rare Categories Count Value",
 "Percentage of Outlier Records Value",
 "Outlier Values Value",
 "Data Quality Classification Value"
 )
)

# COMMAND ----------



# COMMAND ----------

# Define parameters for function calls
# parameters = [
#     {
#         "table_name": "beliv_prd.commercial.nocbc_sales",
#         "num_list": ["ml_per_unit", "units_per_box", "relation", "sales", "sales_units", "cases_beliv", "cases_distributor", "year"],
#         "cat_list": ["distributor", "client_code", "client_name", "channel", "category", "external_sku", "external_sku_description",
#                      "internal_sku", "internal_sku_description", "brand", "package", "flavor", "company", "client_code_company",
#                      "address_client", "client_type", "client_office", "currency", "original_company", "delegacion", "seller"],
#         "date_list": ["report_date", "sales_date", "ingest_date"],
#         "rare_threshold": 0.005
#     },

#     {
#         'table_name': 'beliv_prd.commercial.nocbc_inventories',
#         'num_list': ['ml_per_unit', 'units_per_box', 'relation', 'inventory_cases', 'year'],
#         'cat_list': [
#             'distributor', 'company', 'client_code_company', 'category', 'external_sku', 
#             'external_sku_description', 'internal_sku', 'internal_sku_description', 'brand', 
#             'package', 'flavor', 'level', 'warehouse'
#         ],
#         'date_list': ['report_date', 'inventory_date', 'ingest_date'],
#         'rare_threshold': 0.005
#     }
    # ,

    # {
    #     'table_name': 'beliv_prd.commercial.modelos_negocio',
    #     'num_list': [
    #         'stock_days', 'percentage_cost', 'percentage_price', 'unique_code_financial', 
    #         'vendor_code', 'client_code_custom', 'year', 'last_updated'
    #     ],
    #     'cat_list': [
    #         'id_model', 'soc_beliv', 'soc_gl_sale', 'soc_gl_cost', 'acc_vol_cjf', 'acc_vol_8onz',
    #         'acc_sale', 'acc_cost', 'return', 'sku_type', 'model', 'operating_model', 
    #         'business_model', 'is_kit', 'id_bpc', 'sku_code', 'payment_model', 'sourcing'
    #     ],
    #     'date_list': ['ingest_date'],
    #     'rare_threshold': 0.005
    # }
# ]
# Initialize an empty DataFrame to hold all insights

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# Prepare the outlier data with numeric values
outlier_df = final_insights_df.filter(final_insights_df["Metric Name"] == "Percentage of Outlier Records") \
    .withColumn("Numeric Metric Value", regexp_replace(final_insights_df["Metric Value"], "%", "").cast("double")) \
    .alias("outliers")

# Alias the `final_insights_df` as `columns` for the join
columns_df = final_insights_df.alias("columns")

# Perform self-join on Table ID to fetch all columns from columns_df
result_df = outlier_df.join(
    columns_df,
    on=outlier_df["Column ID"] == columns_df["Column ID"],  # Explicit join condition to avoid ambiguity
    how="inner"
).filter(
    columns_df["Metric Name"].isin("Percentage of Outlier Records", "Column name")
).select(
    *[f"columns.{col}" for col in columns_df.columns]  # Select all columns from `columns_df`, qualified with the alias
)

# Show the result
# result_df.show()
display(result_df.distinct().orderBy("Column ID", "Metric Name", ascending=False))
# display(result_df.select(columns_df["Metric Name"]).distinct())
