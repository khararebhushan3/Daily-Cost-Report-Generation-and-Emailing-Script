## Daily Cost Report Generation and Emailing Script

### Overview
This script generates a daily cost report from a database, formats the data into an HTML table, and sends the report via email. The report includes Year-To-Date (YTD) and Month-To-Date (MTD) cost metrics for different locations and brands. The script uses `pandas` for data manipulation, `SQLAlchemy` for database connectivity, and `smtplib` for sending emails.

### Detailed Explanation

#### Importing Libraries
```python
import pandas as pd
import pymysql
pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from datetime import datetime, timedelta
import resource_files.sql_connector as sqlc
import resource_files.emails as email_conf
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
```
- **pandas**: Used for data manipulation and analysis.
- **pymysql**: MySQL driver.
- **SQLAlchemy**: SQL toolkit and Object-Relational Mapping (ORM) library.
- **datetime**: Used for handling dates and times.
- **smtplib**: Python library for sending emails.
- **email.mime**: Used to construct the email with HTML content.

#### SQL Query to Fetch Data
```python
sql_script_downstream_cost = """
WITH LatestInvoiceEntries AS (
    SELECT
        r.RONumber,
        i.InvoiceNo,
        MAX(i.InvoiceDate) AS LatestInvoiceDate
    FROM lis_plus_2_0.Service_ROInvoice i
    LEFT JOIN Service_ROForm r ON i.ROID = r.Id AND r.isdeleted = 0
    GROUP BY r.RONumber, i.InvoiceNo
)
SELECT DISTINCT 
    i.InvoiceNo, 
    c.name AS company_name, 
    l.name AS location_name, 
    r.RONumber, 
    i.InvoiceDate AS InvoiceDate, 
    cu.CustomerName,
    m.name AS model_name, 
    va.name AS variant_name, 
    v.Vin, 
    ca.name AS group_name,
    pr.product_code AS item_name, 
    pr.hsn_code, 
    p.TaxRate, 
    p.IssuedQty, 
    pr.purchase_price,
    la.SACCode,
    lm.LabourDesc,
    r.total_labour_taxable_amount,
    pr.MRP * p.IssuedQty AS MRP, 
    ((pr.MRP * p.IssuedQty) / ((100 + p.TaxRate) / 100)) AS mrp_without_tax, 
    p.Discount, 
    (p.TotalAmount / ((100 + p.TaxRate) / 100)) AS without_gst_sell_amount, 
    i.GrandTotal,
    r.Ro_Type
FROM Service_ROInvoice i
LEFT JOIN Service_ROForm r ON i.ROID = r.Id AND r.isdeleted = 0
LEFT JOIN Service_tblAccdetails p ON p.ROID = i.ROID AND p.isdeleted = 0
LEFT JOIN Service_tblLabourDetails la ON la.RoId = r.Id AND la.isdeleted = 0
LEFT JOIN Service_lstLabourMaster lm ON lm.id = la.labourId AND lm.isdeleted = 0
LEFT JOIN companies c ON c.Id = r.Company_Id
LEFT JOIN locations l ON l.Id = r.Location_Id
LEFT JOIN LG_CustomerDetails_Log cu ON cu.id = r.CustomerLog_id
LEFT JOIN Service_VehicleMaster v ON v.ID = r.Vehicle_Id
LEFT JOIN models m ON m.Id = v.Model_id
LEFT JOIN variants va ON va.Id = v.Varient_Id
LEFT JOIN products pr ON pr.id = p.ProductId
LEFT JOIN categories ca ON ca.id = pr.category_id
JOIN LatestInvoiceEntries lie ON i.InvoiceNo = lie.InvoiceNo AND i.InvoiceDate = lie.LatestInvoiceDate
"""
```
- The SQL script retrieves invoice data, customer information, vehicle details, and product costs from a database.

#### Database Connection and Data Fetching
```python
engine = create_engine(f"{sqlc.LMG_LDB_CONNECTION}://{sqlc.LMG_LDB_USERNAME}:{sqlc.LMG_LDB_PASSWORD}@{sqlc.LMG_LDB_HOST}:{sqlc.LMG_LDB_PORT}/{sqlc.LMG_LDB_DATABASE}")

try:
    df_downstream = pd.read_sql_query(sql_script_downstream_cost, engine, parse_dates=["InvoiceDate"])
except Exception as e:
    print(f"Error: {e}")

df_downstream.info()
```
- **create_engine**: Constructs a SQLAlchemy engine for connecting to the database.
- **pd.read_sql_query**: Executes the SQL query and reads the result into a pandas DataFrame.

#### Data Cleaning and Transformation
```python
# Update company_name
company_replacements = {
    "Landmark Select - AMPL": "VW",
    "WCPL-A": "Honda",
    "Motorone India Private Limited": "Honda"
}
df_downstream.loc[:, 'company_name'] = df_downstream['company_name'].replace(company_replacements)

# Update location_name
location_replacements = {
    "AH-SARKHEJ": "Ahmedabad",
    "Ambli": "Ahmedabad",
    "Andheri WORKSHOP": "Mumbai",
    "Indore Workshop": "Indore",
    "Isanpur": "Ahmedabad",
    "MULUND WEST": "Mumbai",
    "NAROL-WORKSHOP": "Ahmedabad",
    "Navsari": "Ahmedabad",
    "Nerul Workshop": "Mumbai",
    "PANJIM WS": "Goa",
    "Sola": "Ahmedabad",
    "Surat Workshop": "Surat",
    "Thaltej": "Ahmedabad",
    "Thane Workshop": "Mumbai",
    "Udhna": "Surat",
    "Vapi": "Ahmedabad",
    "VILE PARLE ( WEST )": "Mumbai"
}
df_downstream.loc[:, 'location_name'] = df_downstream['location_name'].replace(location_replacements)

# Filter group_name
df_downstream = df_downstream[df_downstream['group_name'].isin(['LGA', 'M1'])]

# Ensure 'purchase_price' and 'IssuedQty' are not NaN for calculation
df_downstream['cost'] = df_downstream['purchase_price'] * df_downstream['IssuedQty']

print(df_downstream.head())
```
- **replace**: Updates company and location names for consistency.
- **isin**: Filters the DataFrame to include only specified group names.
- **cost**: Calculates the cost by multiplying the purchase price with the issued quantity.

#### Email Generation and Sending
```python
def send_email(df):
    current_datetime = datetime.now()
    ist_datetime = current_datetime + timedelta(hours=5, minutes=30)
    current_date_str = ist_datetime.strftime("%Y_%m_%d_%H_%M")
    today = ist_datetime.date()

    html_sections = []

    for group_name in df['group_name'].unique():
        group_df = df[df['group_name'] == group_name]
        brand_sections = []

        for brand in group_df['company_name'].unique():
            brand_df = group_df[group_df['company_name'] == brand]

            pivot_table_ytd = brand_df.pivot_table(
                index='location_name',
                values=['cost'],
                aggfunc='sum'
            ).rename(columns={'cost': 'YTD Cost'})

            pivot_table_mtd = brand_df[brand_df['InvoiceDate'].dt.month == today.month].pivot_table(
                index='location_name',
                values=['cost'],
                aggfunc='sum'
            ).rename(columns={'cost': 'MTD Cost'})

            merged_pivot_table = pd.concat([pivot_table_ytd, pivot_table_mtd], axis=1)
            merged_pivot_table = merged_pivot_table.applymap(lambda x: "{:,.0f}".format(x) if not pd.isna(x) else x)

            table_html = '<table class="table table-bordered" style="text-align: center;">'
            table_html += '<thead>'
            table_html += '<tr>'
            table_html += '<th rowspan="2">Location</th>'
            table_html += '<th colspan="2">YTD</th>'
            table_html += '<th colspan="2">MTD</th>'
            table_html += '</tr>'
            table_html += '<tr>'
            table_html += '<th>Cost</th>'
            table_html += '<th>Cost</th>'
            table_html += '</tr>'
            table_html += '</thead>'
            table_html += '<tbody>'

            for location in merged_pivot_table.index:
                table_html += '<tr>'
                table_html += f'<td>{location}</td>'
                table_html += f'<td>{merged_pivot_table.at[location, "YTD Cost"]}</td>'
                table_html += f'<td>{merged_pivot_table.at[location, "MTD Cost"]}</td>'
                table_html += '</tr>'

            table_html += '</tbody>'
            table_html += '</table>'

            brand_section = f"""
            <h3>Brand: {brand}</h3>
            <h4>Cost Report</h4>
            {table_html}
            """
            brand_sections.append(brand_section)

        full_brand_section = "".join(brand_sections)

        html_section = f"""
        <h2>Product: {group_name}</h2>
        {full_brand_section}
        """
        html_sections.append(html_section)

    full_html_body =

 "".join(html_sections)

    msg = MIMEMultipart()
    msg['From'] = email_conf.username
    msg['To'] = ", ".join(email_conf.TO_EMAILS)
    msg['Cc'] = ", ".join(email_conf.CC_EMAILS)
    msg['Subject'] = f"Daily Cost Report - {current_date_str}"

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
        <style>
            table {{ margin: auto; border-collapse: collapse; width: 100%; }}
            th, td {{ text-align: center; padding: 8px; border: 1px solid black; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <p>Hi everyone,</p>
        <p>Please check the report below for today's cost report.</p>
        {full_html_body}
        <p>Regards,<br>Data Products Team<br>Landmark Transformation Team</p>
        <p><br><br>Note-This is an auto-generated email.</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP(email_conf.smtp_server, email_conf.smtp_port) as server:
        server.starttls()
        server.login(email_conf.username, email_conf.password)
        server.sendmail(email_conf.username, email_conf.TO_EMAILS + email_conf.CC_EMAILS, msg.as_string())

    return None
```
- **send_email**: Constructs and sends the email with the HTML table containing cost metrics.
- **MIMEMultipart** and **MIMEText**: Used to create and attach the HTML content of the email.
- **smtplib.SMTP**: Sends the email using the SMTP server configuration.

#### Additional Functions
```python
def data_fetch():
    return df_downstream

def calculate_warranty_status(df):
    return df

def lambda_handler(event, context):
    send_email(calculate_warranty_status(data_fetch()))

lambda_handler(None, None)
```
- **data_fetch**: Simulates fetching data (returns the pre-fetched DataFrame).
- **calculate_warranty_status**: Placeholder function (returns the input DataFrame).
- **lambda_handler**: Main function to be triggered by AWS Lambda, calls the `send_email` function.

### Conclusion
This script automates the process of generating a daily cost report and emailing it to the specified recipients. It ensures consistency in data formatting and provides a comprehensive view of costs for various locations and brands.
