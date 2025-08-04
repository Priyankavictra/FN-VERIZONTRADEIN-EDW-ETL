import requests
import pyodbc

import logging

import os

class VerizonTradeInETL:
    def create_tables_if_not_exist(self, conn):
        cursor = conn.cursor()
        # Create staging table (all columns as NVARCHAR(255)), add TradeInDate_EST, PostTime_EST, ResponseTime_EST
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport_staging' AND TABLE_SCHEMA = 'verizon')
        BEGIN
            CREATE TABLE verizon.RQTradeinReport_staging (
                SaleInvoiceID NVARCHAR(255),
                TradeInTransactionID NVARCHAR(255),
                InvoiceIDByStore NVARCHAR(255),
                InvoiceID NVARCHAR(255),
                TradeInStatus NVARCHAR(255),
                ItemID NVARCHAR(255),
                ManufacturerModel NVARCHAR(255),
                SerialNumber NVARCHAR(255),
                StoreName NVARCHAR(255),
                RegionName NVARCHAR(255),
                TradeInDate NVARCHAR(255),
                TradeInDate_EST NVARCHAR(255),
                PhoneRebateAmount NVARCHAR(255),
                PromotionValue NVARCHAR(255),
                PreDeviceValueAmount NVARCHAR(255),
                PrePromotionValueAmount NVARCHAR(255),
                TrackingNumber NVARCHAR(255),
                OriginalTradeInvoiceID NVARCHAR(255),
                OrderNumber NVARCHAR(255),
                CreditApplicationNum NVARCHAR(255),
                LocationCode NVARCHAR(255),
                MasterOrderNumber NVARCHAR(255),
                SequenceNumber NVARCHAR(255),
                PromoValue NVARCHAR(255),
                OrganicPrice NVARCHAR(255),
                ComputedPrice NVARCHAR(255),
                TradeInMobileNumber NVARCHAR(255),
                SubmissionId NVARCHAR(255),
                TradeInEquipMake NVARCHAR(255),
                TradeInEquipCarrier NVARCHAR(255),
                DeviceSku NVARCHAR(255),
                TradeInDeviceId NVARCHAR(255),
                LobType NVARCHAR(255),
                OrderType NVARCHAR(255),
                PurchaseDeviceId NVARCHAR(255),
                TradeInAmount NVARCHAR(255),
                AmountUsed NVARCHAR(255),
                AmountPending NVARCHAR(255),
                PromoCompletion NVARCHAR(255),
                PostTime NVARCHAR(255),
                PostTime_EST NVARCHAR(255),
                ResponseTime NVARCHAR(255),
                ResponseTime_EST NVARCHAR(255),
                MobileNumber NVARCHAR(255),
                ETLrowinserted DATETIME DEFAULT GETDATE()
            )
        END
        ''')
        # Create target table with proper data types, add TradeInDate_EST, PostTime_EST, ResponseTime_EST
        cursor.execute('''
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RQTradeinReport' AND TABLE_SCHEMA = 'verizon')
        BEGIN
            CREATE TABLE verizon.RQTradeinReport (
                SaleInvoiceID NVARCHAR(50),
                TradeInTransactionID NVARCHAR(50),
                InvoiceIDByStore NVARCHAR(50),
                InvoiceID NVARCHAR(50),
                TradeInStatus NVARCHAR(50),
                ItemID NVARCHAR(50),
                ManufacturerModel NVARCHAR(100),
                SerialNumber NVARCHAR(100),
                StoreName NVARCHAR(100),
                RegionName NVARCHAR(50),
                TradeInDate NVARCHAR(50),
                TradeInDate_EST NVARCHAR(50),
                PhoneRebateAmount DECIMAL(18,2),
                PromotionValue DECIMAL(18,2),
                PreDeviceValueAmount DECIMAL(18,2),
                PrePromotionValueAmount DECIMAL(18,2),
                TrackingNumber NVARCHAR(100),
                OriginalTradeInvoiceID NVARCHAR(50),
                OrderNumber NVARCHAR(50),
                CreditApplicationNum NVARCHAR(50),
                LocationCode NVARCHAR(20),
                MasterOrderNumber NVARCHAR(50),
                SequenceNumber INT,
                PromoValue DECIMAL(18,2),
                OrganicPrice DECIMAL(18,2),
                ComputedPrice DECIMAL(18,2),
                TradeInMobileNumber NVARCHAR(20),
                SubmissionId NVARCHAR(50),
                TradeInEquipMake NVARCHAR(50),
                TradeInEquipCarrier NVARCHAR(50),
                DeviceSku NVARCHAR(50),
                TradeInDeviceId NVARCHAR(50),
                LobType NVARCHAR(20),
                OrderType NVARCHAR(20),
                PurchaseDeviceId NVARCHAR(50),
                TradeInAmount DECIMAL(18,2),
                AmountUsed DECIMAL(18,2),
                AmountPending DECIMAL(18,2),
                PromoCompletion NVARCHAR(20),
                PostTime NVARCHAR(50),
                PostTime_EST NVARCHAR(50),
                ResponseTime NVARCHAR(50),
                ResponseTime_EST NVARCHAR(50),
                MobileNumber NVARCHAR(20),
                ETLrowinserted DATETIME DEFAULT GETDATE(),
                ETLrowupdated DATETIME
            )
        END
        ''')
        # Add TradeInDate_EST, PostTime_EST, ResponseTime_EST columns to existing tables if missing
        for col, dtype in [
            ("TradeInDate_EST", "NVARCHAR(255)"),
            ("PostTime_EST", "NVARCHAR(255)"),
            ("ResponseTime_EST", "NVARCHAR(255)")
        ]:
            try:
                cursor.execute(f"ALTER TABLE verizon.RQTradeinReport_staging ADD {col} {dtype};")
            except Exception as e:
                logging.warning(f"Could not add {col} to staging: {e}")
        for col, dtype in [
            ("TradeInDate_EST", "NVARCHAR(50)"),
            ("PostTime_EST", "NVARCHAR(50)"),
            ("ResponseTime_EST", "NVARCHAR(50)")
        ]:
            try:
                cursor.execute(f"ALTER TABLE verizon.RQTradeinReport ADD {col} {dtype};")
            except Exception as e:
                logging.warning(f"Could not add {col} to target: {e}")
        # Alter columns in target table to proper types if table already exists
        alter_sqls = [
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN SaleInvoiceID NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInTransactionID NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN InvoiceIDByStore NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN InvoiceID NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInStatus NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN ItemID NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN ManufacturerModel NVARCHAR(100);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN SerialNumber NVARCHAR(100);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN StoreName NVARCHAR(100);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN RegionName NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInDate NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PhoneRebateAmount DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PromotionValue DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PreDeviceValueAmount DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PrePromotionValueAmount DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TrackingNumber NVARCHAR(100);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN OriginalTradeInvoiceID NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN OrderNumber NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN CreditApplicationNum NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN LocationCode NVARCHAR(20);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN MasterOrderNumber NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN SequenceNumber INT;",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PromoValue DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN OrganicPrice DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN ComputedPrice DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInMobileNumber NVARCHAR(20);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN SubmissionId NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInEquipMake NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInEquipCarrier NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN DeviceSku NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInDeviceId NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN LobType NVARCHAR(20);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN OrderType NVARCHAR(20);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PurchaseDeviceId NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN TradeInAmount DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN AmountUsed DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN AmountPending DECIMAL(18,2);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PromoCompletion NVARCHAR(20);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN PostTime NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN ResponseTime NVARCHAR(50);",
            "ALTER TABLE verizon.RQTradeinReport ALTER COLUMN MobileNumber NVARCHAR(20);"
        ]
        for sql in alter_sqls:
            try:
                cursor.execute(sql)
            except Exception as e:
                logging.warning(f"Could not alter column with SQL: {sql} Error: {e}")
        conn.commit()
    def get_sql_connection(self):
        conn_str = os.environ.get("CONNECTION_STRING")
        if not conn_str:
            raise ValueError("CONNECTION_STRING environment variable not set.")
        return pyodbc.connect(conn_str)

    def fetch_api_data(self, params):
        # Use params['StartDate'] and params['StopDate'] dynamically
        url = f"https://dataconnect.iqmetrix.net/Reports/TradeInActivityReport?ProviderID=18&StartDate={params['StartDate']}&StopDate={params['StopDate']}&LocationType=Company&LocationTypeIDs=-1&CompanyID=13325"
        headers = {
            "Authorization": "Basic cG90YXRvLmV0bEB2aWN0cmEuY29tOjxuUVhmNSwjIXxoZj84T29hTUJe",
            "Cookie": "ApplicationGatewayAffinity=36f3760ed0711848f486da1adf16ec68; ApplicationGatewayAffinityCORS=36f3760ed0711848f486da1adf16ec68; DataConnectState=pnyjjmvt2zkwjkbtn42iteui"
        }
        logging.info(f"Fetching API data for {params['StartDate']} to {params['StopDate']}...")
        response = requests.get(url, headers=headers)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logging.error(f"API request failed: {e}. Response: {response.text}")
            raise
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
        import datetime
        def to_est(raw):
            if not raw:
                return None
            import re
            cleaned = raw.replace('T', ' ').replace('Z', '')
            dt = None
            # Try parsing with microseconds
            try:
                dt = datetime.datetime.strptime(cleaned, '%Y-%m-%d %H:%M:%S.%f')
            except Exception:
                # Try parsing without microseconds
                try:
                    dt = datetime.datetime.strptime(cleaned, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    # Try parsing with regex for edge cases
                    match = re.match(r'(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?', cleaned)
                    if match:
                        date_part = match.group(1)
                        time_part = match.group(2)
                        micro_part = match.group(3) if match.group(3) else ''
                        try:
                            dt = datetime.datetime.strptime(f"{date_part} {time_part}", '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            dt = None
            if dt:
                est = dt - datetime.timedelta(hours=5)
                # If original had microseconds, add them back
                if '.' in cleaned:
                    micro = cleaned.split('.')[-1]
                    return est.strftime('%Y-%m-%d %H:%M:%S.%f')[:23]
                else:
                    return est.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return cleaned
        for row in data:
            valid_row = {k: row.get(k, None) for k in table_columns}
            # Always use original API value for main columns (preserve T and Z)
            def ensure_tz(val):
                if not val:
                    return val
                # If already has T and Z, return as is
                if 'T' in val and val.endswith('Z'):
                    return val
                # If has T but not Z, add Z
                if 'T' in val and not val.endswith('Z'):
                    return val + 'Z'
                # If has Z but not T, add T at correct position
                if 'Z' in val and 'T' not in val:
                    parts = val.split(' ')
                    if len(parts) == 2:
                        return parts[0] + 'T' + parts[1]
                    return val
                # If neither, add T and Z
                parts = val.split(' ')
                if len(parts) == 2:
                    return parts[0] + 'T' + parts[1] + 'Z'
                return val
            if 'TradeInDate' in table_columns:
                valid_row['TradeInDate'] = ensure_tz(row.get('TradeInDate', None))
            if 'PostTime' in table_columns:
                valid_row['PostTime'] = ensure_tz(row.get('PostTime', None))
            if 'ResponseTime' in table_columns:
                valid_row['ResponseTime'] = ensure_tz(row.get('ResponseTime', None))

            # Only fill _EST columns if conversion succeeds
            est_tradein = to_est(row.get('TradeInDate', None))
            est_post = to_est(row.get('PostTime', None))
            est_response = to_est(row.get('ResponseTime', None))
            # Always fill EST columns: use EST value if conversion succeeds, else fallback to original value
            valid_row['TradeInDate_EST'] = est_tradein if est_tradein not in [None, '', row.get('TradeInDate', None)] else row.get('TradeInDate', None)
            valid_row['PostTime_EST'] = est_post if est_post not in [None, '', row.get('PostTime', None)] else row.get('PostTime', None)
            valid_row['ResponseTime_EST'] = est_response if est_response not in [None, '', row.get('ResponseTime', None)] else row.get('ResponseTime', None)

            columns = ','.join([k for k in valid_row.keys() if k])
            placeholders = ','.join(['?' for k in valid_row.keys() if k])
            values = [v if v is not None else None for k, v in valid_row.items() if k]
            # Defensive: skip if columns or values are empty or mismatched
            if not columns or not values or len(columns.split(',')) != len(values):
                logging.warning(f"Skipping row due to column/value mismatch: {row}")
                continue
            cursor.execute(f"INSERT INTO verizon.RQTradeinReport_staging ({columns}) VALUES ({placeholders})", values)
            rows_loaded += 1
            rows_loaded += 1
        conn.commit()
        # Clean PostTime and ResponseTime columns in staging table to 'yyyy-MM-dd HH:mm:ss.ffff' format
        conn.commit()
        logging.info(f"Loaded {rows_loaded} rows into staging table.")
        return rows_loaded

    def merge_to_target(self, conn):
        cursor = conn.cursor()
        try:
            merge_columns = [
                'SaleInvoiceID', 'TradeInTransactionID', 'InvoiceIDByStore', 'InvoiceID', 'TradeInStatus', 'ItemID', 'ManufacturerModel', 'SerialNumber', 'StoreName', 'RegionName', 'TradeInDate',
                'TradeInDate_EST', 'PostTime', 'PostTime_EST', 'ResponseTime', 'ResponseTime_EST',
                'PhoneRebateAmount', 'PromotionValue', 'PreDeviceValueAmount', 'PrePromotionValueAmount', 'TrackingNumber', 'OriginalTradeInvoiceID', 'OrderNumber', 'CreditApplicationNum',
                'LocationCode', 'MasterOrderNumber', 'SequenceNumber', 'PromoValue', 'OrganicPrice', 'ComputedPrice', 'TradeInMobileNumber', 'SubmissionId', 'TradeInEquipMake', 'TradeInEquipCarrier',
                'DeviceSku', 'TradeInDeviceId', 'LobType', 'OrderType', 'PurchaseDeviceId', 'TradeInAmount', 'AmountUsed', 'AmountPending', 'PromoCompletion', 'MobileNumber'
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
            # Clean PostTime and ResponseTime columns to 'yyyy-MM-dd HH:mm:ss.ffff' format
            cursor.execute("UPDATE verizon.RQTradeinReport SET PostTime = CASE WHEN TRY_CONVERT(datetime2(4), REPLACE(REPLACE(PostTime, 'T', ' '), 'Z', '')) IS NOT NULL THEN FORMAT(TRY_CONVERT(datetime2(4), REPLACE(REPLACE(PostTime, 'T', ' '), 'Z', '')), 'yyyy-MM-dd HH:mm:ss.ffff') ELSE PostTime END, ResponseTime = CASE WHEN TRY_CONVERT(datetime2(4), REPLACE(REPLACE(ResponseTime, 'T', ' '), 'Z', '')) IS NOT NULL THEN FORMAT(TRY_CONVERT(datetime2(4), REPLACE(REPLACE(ResponseTime, 'T', ' '), 'Z', '')), 'yyyy-MM-dd HH:mm:ss.ffff') ELSE ResponseTime END")
            conn.commit()
            # Get counts
            inserted = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowinserted = CONVERT(date, GETDATE())").fetchval()
            updated = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowupdated = CONVERT(date, GETDATE())").fetchval()
            logging.info(f"Merge complete. Inserted: {inserted}, Updated: {updated}")
            # Delete all records from staging table except those with today's date
            cursor.execute("DELETE FROM verizon.RQTradeinReport_staging WHERE CONVERT(date, ETLrowinserted) <> CONVERT(date, GETDATE())")
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            logging.error(f"Error in merge_to_target: {e}")
            raise
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
        # Delete all records from staging table except those with today's date
        cursor.execute("DELETE FROM verizon.RQTradeinReport_staging WHERE CONVERT(date, ETLrowinserted) <> CONVERT(date, GETDATE())")
        conn.commit()
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
        import datetime, time
        etl_start = time.time()
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
        etl_end = time.time()
        etl_duration = etl_end - etl_start
        logging.info(f"ETL process completed in {etl_duration:.2f} seconds.")
        return {
            "rows_loaded": rows_loaded,
            "merge_stats": merge_stats,
            "validation_result": validation_result,
            "etl_duration_seconds": round(etl_duration, 2)
        }
        try:
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
            # Format PostTime and ResponseTime columns in staging table before merge, safely using TRY_CONVERT
            cursor.execute("UPDATE verizon.RQTradeinReport_staging SET PostTime = CASE WHEN TRY_CONVERT(datetime, PostTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, PostTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE PostTime END, ResponseTime = CASE WHEN TRY_CONVERT(datetime, ResponseTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, ResponseTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE ResponseTime END")
            conn.commit()
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
            # Ensure PostTime and ResponseTime in target table are always formatted
            cursor.execute("UPDATE verizon.RQTradeinReport SET PostTime = CASE WHEN TRY_CONVERT(datetime, PostTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, PostTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE PostTime END, ResponseTime = CASE WHEN TRY_CONVERT(datetime, ResponseTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, ResponseTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE ResponseTime END")
            conn.commit()
            # Reformat all previous PostTime and ResponseTime values in target table to EST and required format
            cursor.execute("UPDATE verizon.RQTradeinReport SET PostTime = CASE WHEN TRY_CONVERT(datetime, PostTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, PostTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE PostTime END, ResponseTime = CASE WHEN TRY_CONVERT(datetime, ResponseTime) IS NOT NULL THEN FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, TRY_CONVERT(datetime, ResponseTime)), '-05:00'), 'MMM dd, yyyy HH:mm:ss') ELSE ResponseTime END")
            conn.commit()
            # Get counts
            inserted = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowinserted = CONVERT(date, GETDATE())").fetchval()
            updated = cursor.execute("SELECT COUNT(*) FROM verizon.RQTradeinReport WHERE ETLrowupdated = CONVERT(date, GETDATE())").fetchval()
            logging.info(f"Merge complete. Inserted: {inserted}, Updated: {updated}")
            # Delete all records from staging table except those with today's date
            cursor.execute("DELETE FROM verizon.RQTradeinReport_staging WHERE CONVERT(date, ETLrowinserted) <> CONVERT(date, GETDATE())")
            conn.commit()
            return {"inserted": inserted, "updated": updated}
        except Exception as e:
            logging.error(f"Error in merge_to_target: {e}")
            raise
