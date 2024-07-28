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
from email.mime.base import MIMEBase
from email import encoders
import os

# SQL script
sql_script_downstream_cost = f"""
WITH latest_invoice_entries_tb AS (
    SELECT
        r.RONumber,
        i.InvoiceNo,
        MAX(i.InvoiceDate) AS LatestInvoiceDate
    FROM lis_plus_2_0.Service_ROInvoice i
    LEFT JOIN Service_ROForm r ON i.ROID = r.Id AND r.isdeleted = 0
    -- WHERE r.created_at > '2024-05-25'
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
JOIN latest_invoice_entries_tb lie ON i.InvoiceNo = lie.InvoiceNo AND i.InvoiceDate = lie.LatestInvoiceDate
-- WHERE r.created_at > '2024-05-25'
;
"""

# Construct the engine string
engine = create_engine(f"{sqlc.LMG_LDB_CONNECTION}://{sqlc.LMG_LDB_USERNAME}:{sqlc.LMG_LDB_PASSWORD}@{sqlc.LMG_LDB_HOST}:{sqlc.LMG_LDB_PORT}/{sqlc.LMG_LDB_DATABASE}")

print(f"engine: {engine}")  # For debugging purposes

# Execute the SQL script and read the data into a DataFrame
try:
    df_downstream = pd.read_sql_query(sql_script_downstream_cost, engine, parse_dates=["InvoiceDate"])
except Exception as e:
    print(f"Error: {e}")

df_downstream.info()

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

# Display the DataFrame
print(df_downstream.head())

# Generate Email
def send_email(df):
    # Get the current date and time
    current_datetime = datetime.now()
    ist_datetime = current_datetime + timedelta(hours=5, minutes=30)
    current_date_str = ist_datetime.strftime("%Y_%m_%d_%H_%M")
    today = ist_datetime.date()

    # Create a list to hold the HTML sections for each group
    html_sections = []

    # Loop through each unique group name
    for group_name in df['group_name'].unique():
        group_df = df[df['group_name'] == group_name]
        # Create a list to hold the HTML sections for each brand within the group
        brand_sections = []
        
        # Loop through each unique brand within the group
        for brand in group_df['company_name'].unique():
            brand_df = group_df[group_df['company_name'] == brand]

            # Create a pivot table for YTD
            pivot_table_ytd = brand_df.pivot_table(
                index='location_name',
                values=['cost'],
                aggfunc='sum'
            ).rename(columns={'cost': 'YTD Cost'})

            # Create a pivot table for MTD
            pivot_table_mtd = brand_df[brand_df['InvoiceDate'].dt.month == today.month].pivot_table(
                index='location_name',
                values=['cost'],
                aggfunc='sum'
            ).rename(columns={'cost': 'MTD Cost'})

            # Merge YTD and MTD pivot tables
            merged_pivot_table = pd.concat([pivot_table_ytd, pivot_table_mtd], axis=1)

            # Format the pivot table as required
            merged_pivot_table = merged_pivot_table.applymap(lambda x: "{:,.0f}".format(x) if not pd.isna(x) else x)

            # Create the HTML table manually to match the desired format
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

            # Create the HTML section for this brand
            brand_section = f"""
            <h3>Brand: {brand}</h3>
            <h4>Cost Report</h4>
            {table_html}
            """
            brand_sections.append(brand_section)

        # Combine all brand sections into one for this group
        full_brand_section = "".join(brand_sections)

        # Create the HTML section for this group
        html_section = f"""
        <h2>Product: {group_name}</h2>
        {full_brand_section}
        """
        html_sections.append(html_section)

    # Combine all HTML sections into one
    full_html_body = "".join(html_sections)

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = email_conf.username
    msg['To'] = ', '.join(email_conf.TO_EMAILS)
    msg['Cc'] = ', '.join(email_conf.CC_EMAILS)
    msg['Subject'] = f"Daily Cost Report - {current_date_str}"

    # Email body with the HTML table
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

    # Send the email via IceWarp SMTP server
    with smtplib.SMTP(email_conf.smtp_server, email_conf.smtp_port) as server:
        server.starttls()  # Use TLS if supported by the server
        server.login(email_conf.username, email_conf.password)
        server.sendmail(email_conf.username, email_conf.TO_EMAILS + email_conf.CC_EMAILS, msg.as_string())

    return None

# Function to simulate fetching data and calculating warranty status
def data_fetch():
    # Simulate fetching data
    return df_downstream

def calculate_warranty_status(df):
    # Simulate calculating warranty status
    return df

def lambda_handler(event, context):
    send_email(calculate_warranty_status(data_fetch()))

# For local testing, uncomment the following line:
lambda_handler(None, None)
