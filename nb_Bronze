# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, expr, rand, when
from pyspark.sql.types import *

# 1. GENERAZIONE TABELLA CRM: Accounts (Anagrafica Clienti CRM)
crm_accounts_data = [
    ("ACC-001", "Acme Corp", "Enterprise", "Milano", "BC-CUST-101"),
    ("ACC-002", "TechNova SRL", "SMB", "Roma", "BC-CUST-102"),
    ("ACC-003", "Global Logistics SpA", "Enterprise", "Torino", "BC-CUST-103"),
    ("ACC-004", "Alfa Consulting", "SMB", "Bologna", "BC-CUST-104"),
]
crm_schema = StructType(
    [
        StructField("AccountID", StringType(), True),
        StructField("AccountName", StringType(), True),
        StructField("CustomerSegment", StringType(), True),
        StructField("City", StringType(), True),
        StructField("BCCustomerCrossID", StringType(), True),
    ]
)

df_crm_accounts = spark.createDataFrame(crm_accounts_data, crm_schema)
df_crm_accounts.write.format("delta").mode("overwrite").saveAsTable(
    "crm_accounts"
)

# 2. GENERAZIONE TABELLA CRM: Opportunities (Pipeline Commerciale)
crm_opps_data = [
    ("OPP-1001", "ACC-001", "Closed Won", 150000.00, "2026-01-15"),
    ("OPP-1002", "ACC-002", "Closed Won", 45000.00, "2026-02-10"),
    ("OPP-1003", "ACC-003", "In Progress", 80000.00, "2026-03-01"),
    ("OPP-1004", "ACC-004", "Closed Lost", 25000.00, "2026-01-20"),
]
opps_schema = StructType(
    [
        StructField("OpportunityID", StringType(), True),
        StructField("AccountID", StringType(), True),
        StructField("Stage", StringType(), True),
        StructField("EstimatedValue", DoubleType(), True),
        StructField("CloseDate", StringType(), True),
    ]
)

df_crm_opps = spark.createDataFrame(crm_opps_data, opps_schema)
df_crm_opps.write.format("delta").mode("overwrite").saveAsTable(
    "crm_opportunities"
)

# 3. GENERAZIONE TABELLA ERP: Customers (Business Central)
bc_customers_data = [
    ("BC-CUST-101", "Acme Corp Ltd", "IT12345678901", 30),
    ("BC-CUST-102", "TechNova Italia", "IT98765432109", 60),
    ("BC-CUST-103", "Global Logistics", "IT45678912304", 30),
    ("BC-CUST-104", "Alfa Consulting", "IT11223344556", 15),
]
bc_cust_schema = StructType(
    [
        StructField("CustomerNo", StringType(), True),
        StructField("CustomerName", StringType(), True),
        StructField("VATRegistrationNo", StringType(), True),
        StructField("PaymentTermsDays", IntegerType(), True),
    ]
)

df_bc_cust = spark.createDataFrame(bc_customers_data, bc_cust_schema)
df_bc_cust.write.format("delta").mode("overwrite").saveAsTable("erp_customers")

# 4. GENERAZIONE TABELLA ERP: Sales Orders & Lines (Business Central)
bc_orders_data = [
    ("ORD-901", "BC-CUST-101", "ITEM-A1", 10, 12000.00, "2026-01-18"),
    ("ORD-902", "BC-CUST-101", "ITEM-B2", 5, 6000.00, "2026-01-19"),
    ("ORD-903", "BC-CUST-102", "ITEM-A1", 4, 11250.00, "2026-02-12"),
]
bc_orders_schema = StructType(
    [
        StructField("OrderNo", StringType(), True),
        StructField("CustomerNo", StringType(), True),
        StructField("ItemNo", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("PostingDate", StringType(), True),
    ]
)

df_bc_orders = spark.createDataFrame(bc_orders_data, bc_orders_schema)
df_bc_orders.write.format("delta").mode("overwrite").saveAsTable(
    "erp_sales_orders"
)

print(
    "✅ Ingestione Bronze completata con successo per CRM ed ERP (Business Central)!"
)
