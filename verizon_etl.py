import requests
import pyodbc

import logging

import os

class VerizonTradeInETL:
    def create_tables_if_not_exist(self, conn):
        cursor = conn.cursor()
        # Create staging table (all columns as NVARCHAR(MAX))
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport_staging' AND TABLE_SCHEMA = 'verizon')
        BEGIN
            CREATE TABLE verizon.RQTradeinReport_staging (
                SaleInvoiceID NVARCHAR(MAX),
                TradeInTransactionID NVARCHAR(MAX),
                InvoiceIDByStore NVARCHAR(MAX),
                InvoiceID NVARCHAR(MAX),
                TradeInStatus NVARCHAR(MAX),
                ItemID NVARCHAR(MAX),
                ManufacturerModel NVARCHAR(MAX),
                SerialNumber NVARCHAR(MAX),
                StoreName NVARCHAR(MAX),
                RegionName NVARCHAR(MAX),
                TradeInDate NVARCHAR(MAX),
                PhoneRebateAmount NVARCHAR(MAX),
                PromotionValue NVARCHAR(MAX),
                PreDeviceValueAmount NVARCHAR(MAX),
                PrePromotionValueAmount NVARCHAR(MAX),
                TrackingNumber NVARCHAR(MAX),
                OriginalTradeInvoiceID NVARCHAR(MAX),
                OrderNumber NVARCHAR(MAX),
                CreditApplicationNum NVARCHAR(MAX),
                LocationCode NVARCHAR(MAX),
                MasterOrderNumber NVARCHAR(MAX),
                SequenceNumber NVARCHAR(MAX),
                PromoValue NVARCHAR(MAX),
                OrganicPrice NVARCHAR(MAX),
                ComputedPrice NVARCHAR(MAX),
                TradeInMobileNumber NVARCHAR(MAX),
                SubmissionId NVARCHAR(MAX),
                TradeInEquipMake NVARCHAR(MAX),
                TradeInEquipCarrier NVARCHAR(MAX),
                DeviceSku NVARCHAR(MAX),
                TradeInDeviceId NVARCHAR(MAX),
                LobType NVARCHAR(MAX),
                OrderType NVARCHAR(MAX),
                PurchaseDeviceId NVARCHAR(MAX),
                TradeInAmount NVARCHAR(MAX),
                AmountUsed NVARCHAR(MAX),
                AmountPending NVARCHAR(MAX),
                PromoCompletion NVARCHAR(MAX),
                PostTime NVARCHAR(MAX),
                ResponseTime NVARCHAR(MAX),
                MobileNumber NVARCHAR(MAX),
                ETLrowinserted DATETIME DEFAULT GETDATE()
            )
        END
        ''')
        # Create target table (all columns as NVARCHAR(MAX), plus ETLrowinserted and ETLrowupdated)
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport' AND TABLE_SCHEMA = 'verizon')
        BEGIN
            CREATE TABLE verizon.RQTradeinReport (
                SaleInvoiceID NVARCHAR(MAX),
                TradeInTransactionID NVARCHAR(MAX),
                InvoiceIDByStore NVARCHAR(MAX),
                InvoiceID NVARCHAR(MAX),
                TradeInStatus NVARCHAR(MAX),
                ItemID NVARCHAR(MAX),
                ManufacturerModel NVARCHAR(MAX),
                SerialNumber NVARCHAR(MAX),
                StoreName NVARCHAR(MAX),
                RegionName NVARCHAR(MAX),
                TradeInDate NVARCHAR(MAX),
                PhoneRebateAmount NVARCHAR(MAX),
                PromotionValue NVARCHAR(MAX),
                PreDeviceValueAmount NVARCHAR(MAX),
                PrePromotionValueAmount NVARCHAR(MAX),
                TrackingNumber NVARCHAR(MAX),
                OriginalTradeInvoiceID NVARCHAR(MAX),
                OrderNumber NVARCHAR(MAX),
                CreditApplicationNum NVARCHAR(MAX),
                LocationCode NVARCHAR(MAX),
                MasterOrderNumber NVARCHAR(MAX),
                SequenceNumber NVARCHAR(MAX),
                PromoValue NVARCHAR(MAX),
                OrganicPrice NVARCHAR(MAX),
                ComputedPrice NVARCHAR(MAX),
                TradeInMobileNumber NVARCHAR(MAX),
                SubmissionId NVARCHAR(MAX),
                TradeInEquipMake NVARCHAR(MAX),
                TradeInEquipCarrier NVARCHAR(MAX),
                DeviceSku NVARCHAR(MAX),
                TradeInDeviceId NVARCHAR(MAX),
                LobType NVARCHAR(MAX),
                OrderType NVARCHAR(MAX),
                PurchaseDeviceId NVARCHAR(MAX),
                TradeInAmount NVARCHAR(MAX),
                AmountUsed NVARCHAR(MAX),
                AmountPending NVARCHAR(MAX),
                PromoCompletion NVARCHAR(MAX),
                PostTime NVARCHAR(MAX),
                ResponseTime NVARCHAR(MAX),
                MobileNumber NVARCHAR(MAX),
                ETLrowinserted DATETIME DEFAULT GETDATE(),
                ETLrowupdated DATETIME
            )
        END
        ''')
        conn.commit()
    def get_sql_connection(self):
        conn_str = os.environ.get("CONNECTION_STRING")
        if not conn_str:
            raise ValueError("CONNECTION_STRING environment variable not set.")
        return pyodbc.connect(conn_str)

    def fetch_api_data(self, params):
        # Use params['StartDate'] and params['StopDate'] dynamically
        url = f"https://dataconnect.iqmetrix.net/Reports/TradeInActivityReport?ProviderID=18&StartDate={params['StartDate']}&StopDate={params['StopDate']}&LocationType=Company&LocationTypeIDs=-1&ComplanyID=13325"
        headers = {
            "Authorization": "Basic cG90YXRvLmV0bEB2aWN0cmEuY29tOjxuUVhmNSwjIXxoZj84T29hTUJe",
            "Cookie": "ApplicationGatewayAffinity=36f3760ed0711848f486da1adf16ec68; ApplicationGatewayAffinityCORS=36f3760ed0711848f486da1adf16ec68; DataConnectState=pnyjjmvt2zkwjkbtn42iteui"
        }
        logging.info(f"Fetching API data for {params['StartDate']} to {params['StopDate']}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched {len(data)} records from API.")
        return data

    def load_staging_data(self, conn, data):
        cursor = conn.cursor()
        rows_loaded = 0
        logging.info(f"Loading data into staging table...")
        # Log actual columns in the table
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'RQTradeinReport_staging' AND TABLE_SCHEMA = 'verizon'")
        table_columns = [row[0] for row in cursor.fetchall()]
        logging.info(f"Actual columns in verizon.RQTradeinReport_staging: {table_columns}")
        # Only insert columns that exist in the table
        for row in data:
            valid_row = {k: row.get(k, None) for k in table_columns}
            columns = ','.join(valid_row.keys())
            placeholders = ','.join(['?' for _ in valid_row])
            values = [v if v is not None else None for v in valid_row.values()]
            cursor.execute(f"INSERT INTO verizon.RQTradeinReport_staging ({columns}) VALUES ({placeholders})", values)
            rows_loaded += 1
        conn.commit()
        logging.info(f"Loaded {rows_loaded} rows into staging table.")
        return rows_loaded

    def merge_to_target(self, conn):
        cursor = conn.cursor()
        logging.info("Merging data from staging to target table...")
        # Merge from staging to target
        # Use CTE to deduplicate source rows by SaleInvoiceID
        merge_columns = [
            'SaleInvoiceID', 'TradeInTransactionID', 'InvoiceIDByStore', 'InvoiceID', 'TradeInStatus', 'ItemID', 'ManufacturerModel', 'SerialNumber', 'StoreName', 'RegionName', 'TradeInDate',
            'PhoneRebateAmount', 'PromotionValue', 'PreDeviceValueAmount', 'PrePromotionValueAmount', 'TrackingNumber', 'OriginalTradeInvoiceID', 'OrderNumber', 'CreditApplicationNum',
            'LocationCode', 'MasterOrderNumber', 'SequenceNumber', 'PromoValue', 'OrganicPrice', 'ComputedPrice', 'TradeInMobileNumber', 'SubmissionId', 'TradeInEquipMake', 'TradeInEquipCarrier',
            'DeviceSku', 'TradeInDeviceId', 'LobType', 'OrderType', 'PurchaseDeviceId', 'TradeInAmount', 'AmountUsed', 'AmountPending', 'PromoCompletion', 'PostTime', 'ResponseTime', 'MobileNumber'
        ]
        update_set = ',\n                '.join([f"target.{col} = source.{col}" for col in merge_columns]) + ",\n                target.ETLrowupdated = GETDATE()"
        insert_cols = ', '.join(merge_columns + ['ETLrowinserted'])
        insert_vals = ', '.join([f"source.{col}" for col in merge_columns] + ['GETDATE()'])
        merge_sql = f'''
        WITH DedupedSource AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY SaleInvoiceID ORDER BY TradeInDate DESC) AS rn
            FROM verizon.RQTradeinReport_staging
        )
        MERGE verizon.RQTradeinReport AS target
        USING (SELECT * FROM DedupedSource WHERE rn = 1) AS source
        ON target.SaleInvoiceID = source.SaleInvoiceID
        WHEN MATCHED THEN
            UPDATE SET
                {update_set}
        WHEN NOT MATCHED THEN
            INSERT (
                {insert_cols}
            )
            VALUES (
                {insert_vals}
            );
        '''
        cursor.execute(merge_sql)
        conn.commit()
        # Get counts
        inserted = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowinserted = CONVERT(date, GETDATE())").fetchval()
        updated = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowupdated = CONVERT(date, GETDATE())").fetchval()
        logging.info(f"Merge complete. Inserted: {inserted}, Updated: {updated}")
        return {"inserted": inserted, "updated": updated}

    def validate_data(self, conn, api_data):
        cursor = conn.cursor()
        # Example: compare count of SaleInvoiceID in API vs target table for the date range
        api_count = len(api_data)
        # Adjust date column as needed for your schema
        target_count = cursor.execute("""
            SELECT COUNT(*) FROM verizon.RQTradeinReport
            WHERE TradeInDate >= ? AND TradeInDate <= ?
        """, api_data[0]['TradeInDate'], api_data[-1]['TradeInDate']).fetchval() if api_data else 0
        validation_passed = api_count == target_count
        logging.info(f"Validation: API count = {api_count}, Target count = {target_count}, Passed = {validation_passed}")
        # Optionally, return sample records for inspection
        sample_records = api_data[:3] if api_data else []
        return {
            "validation_passed": validation_passed,
            "api_count": api_count,
            "target_count": target_count,
            "sample_records": sample_records
        }

    def run_etl(self, params):
        import datetime
        # Set date range to yesterday for each run
        stop_date = datetime.date.today()
        start_date = stop_date - datetime.timedelta(days=1)
        params['StartDate'] = start_date.strftime('%Y-%m-%d')
        params['StopDate'] = stop_date.strftime('%Y-%m-%d')
        logging.info(f"Starting ETL for {params['StartDate']} to {params['StopDate']}")
        conn = self.get_sql_connection()
        self.create_tables_if_not_exist(conn)
        data = self.fetch_api_data(params)
        rows_loaded = self.load_staging_data(conn, data)
        merge_stats = self.merge_to_target(conn)
        validation_result = self.validate_data(conn, data)
        logging.info("ETL process completed.")
        return {
            "status": "success",
            "date_range": f"{params['StartDate']} to {params['StopDate']}",
            "api_records": len(data),
            "staging_loaded": rows_loaded,
            "target_inserted": merge_stats["inserted"],
            "target_updated": merge_stats["updated"],
            "validation_passed": validation_result["validation_passed"],
            "api_count": validation_result["api_count"],
            "target_count": validation_result["target_count"],
            "sample_records": validation_result["sample_records"]
        }
