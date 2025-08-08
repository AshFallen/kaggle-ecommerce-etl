import pandas as pd
import boto3
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
import re
import os
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

postgres_user = os.environ['POSTGRES_USER']
postgres_pass = os.environ['POSTGRES_PASS']
postgres_host = os.environ['POSTGRES_HOST']
postgres_db = os.environ['POSTGRES_DB']
s3_bucket = os.environ['S3_BUCKET']

try:
    s3 = boto3.client('s3')
    logger.info("Initialized S3 client")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {str(e)}")
    raise

def inspect_df(df, name="DataFrame"):
    print(f"\n======= Inspecting {name} =======")
    print("Original columns:", df.columns.to_list())
    logger.debug(f"Original columns: {df.columns.to_list()}")
    print("\n======= DF INFO =======")
    df.info()
    logger.debug(f"Dataframe info:\n{df.info()}")
    print("\n======= DF DESCRIBE =======")
    print(df.describe(include='all'))
    logger.debug(f"Description:\n{df.describe(include='all')}")
    print("\n======= SUM OF BLANKS (NaNs) =======")
    print(df.isnull().sum())
    logger.debug(f"Null values:\n{df.isnull().sum()}")

def is_all_strings(row):
    try:
        return all(
            isinstance(cell, str) and re.search(r'[a-zA-Z]', cell)
            if isinstance(cell, str)
            else False
            for cell in row
        )
    except Exception as e:
        logger.error(f"Error in is_all_strings: {str(e)}")
        return False

def standardize_text_columns(df):
    """Standardize text columns containing specific keywords."""
    try:
        target_keywords = ['sku', 'customer', 'style', 'size']
        for col in df.columns:
            if any(keyword in col.lower() for keyword in target_keywords):
                try:
                    df[col] = df[col].astype(str).str.strip().str.upper()
                    logger.debug(f"Standardized text column: {col}")
                except Exception as e:
                    logger.warning(f"Could not standardize column {col}: {str(e)}")
                    pass
        return df
    except Exception as e:
        logger.error(f"Error in standardize_text_columns: {str(e)}")
        return df

def get_latest_sale_files(minutes=10):
    try:
        logger.info("Extracting the latest files from S3...")

        prefix = f'kaggle/raw/'
        time_threshold = datetime.utcnow() - timedelta(minutes=minutes)
        timestamp_pattern = re.compile(r'{}_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv')
       
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

        if 'Contents' not in response or not response['Contents']:
            logger.warning("No files found in the S3 bucket with the given prefix.")
            return []
        
        recent_files = []

        for obj in response['Contents']:
            key = obj['Key']
            match = timestamp_pattern.search(key)
            try:
                if match:
                    file_time = datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S") 
                else:
                    file_time = obj['LastModified'].replace(tzinfo=None)
            except Exception as e:
                logger.warning(f"Filename timestamp parse failed for {key}: {e}")
                continue

            if file_time > time_threshold:
                logger.info(f"File {key} is within the time window.")
                obj = s3.get_object(Bucket=s3_bucket, Key=key)
                logger.info(f"File size: {obj['ContentLength'] / 1024:.2f} KB")

                try:
                    with io.TextIOWrapper(obj['Body'], encoding='utf-8') as stream:
                        df = pd.read_csv(stream, low_memory=False)
                except UnicodeDecodeError:
                    logger.warning(f"UTF-8 failed for {key}, retrying with ISO-8859-1")
                    obj = s3.get_object(Bucket=s3_bucket, Key=key)  # reload object
                    with io.TextIOWrapper(obj['Body'], encoding='ISO-8859-1') as stream:
                        df = pd.read_csv(stream, low_memory=False)

                recent_files.append({"key": key, "df": df})

        return recent_files
    except Exception as e:
        logger.error(f"Failed to fetch the latest files from s3: {e}")
        raise

def transform(df):
    try:
        logger.info("Starting dataframe transformation")
        logger.debug(f"Initial shape: {df.shape}")
        logger.debug(f"Initial columns: {df.columns.tolist()}")

        # rename columns
        month_mapping = {
            'jan': 'January', 'feb': 'February', 'mar': 'March',
            'apr': 'April', 'may': 'May', 'jun': 'June',
            'jul': 'July', 'aug': 'August', 'sep': 'September',
            'oct': 'October', 'nov': 'November', 'dec': 'December'
        }

        na_values = [' ', '', 'NA', 'na', 'n/a', 'N/A', 'n/A', 'N/a', 'null', 'Null', 'NULL']

        try:
            df.columns = df.columns.str.lower().str.strip().str.replace(r'[\s\-]+','_', regex=True)
            logger.debug("Standardized column names")
        except Exception as e:
            logger.error(f"Error standardizing column names: {str(e)}")
            raise

        for col in df.columns:
            try:
                # Handle date columns
                if 'date' in col:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                        logger.debug(f"Formatted date column: {col}")
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse date column {col}: {str(e)}")
                        pass

                if 'month' in col:
                    try:
                        month_parsed = pd.to_datetime(df[col], errors='coerce')
                        if month_parsed.notna().any():
                            df[col] = month_parsed.dt.strftime('%B')
                            logger.debug(f"Formatted month column {col} as datetime")
                        else:
                            df[col] = df[col].apply(
                                lambda val: month_mapping.get(val.lower().strip()[:3], pd.NA) 
                                if pd.notna(val) and isinstance(val, str) 
                                else pd.NA
                            )
                            logger.debug(f"Formatted month column {col} using mapping")
                    except Exception as e:
                        logger.warning(f"Error processing month column {col}: {str(e)}")
                        continue

                # Handle numeric columns
                if df[col].dtype == 'object':
                    try:
                        cleaned = df[col].astype(str).str.replace(r'[\$,()\s]', '', regex=True)
                        converted = pd.to_numeric(cleaned, errors='coerce')
                        if converted.notna().sum() > 0.9 * len(df):
                            df[col] = converted.round(2)
                            logger.debug(f"Converted numeric column: {col}")
                    except Exception as e:
                        logger.warning(f"Could not convert column {col} to numeric: {str(e)}")
                        pass
            except Exception as e:
                logger.error(f"Error processing column {col}: {str(e)}")
                continue

        try:
            for col in df.select_dtypes(include=['object']):
                df[col] = df[col].astype(str).str.strip()
                logger.debug("Standardized all text columns")
        except Exception as e:
            logger.error(f"Error standardizing text columns: {str(e)}")

        try:    
            df = df.replace(na_values, pd.NA)
            logger.debug("Standardized NA values")
        except Exception as e:
            logger.error(f"Error standardizing NA values: {str(e)}")
            
        try:
            df = df.dropna(axis=1, how='all')
            logger.info(f"Final shape after dropping all-NA columns: {df.shape}")
            logger.debug(f"Final columns: {df.columns.to_list()}")
        except Exception as e:
            logger.error(f"Error dropping all-NA columns: {str(e)}")
        
        print("======= List all Column names =======\n", df.columns.to_list())
        return df
    
    except Exception as e:
        logger.error(f"Error in transform function: {str(e)}")
        return df
    
def clean_amazon_sale(df):
    try:
        logger.info("Starting Amazon sales data cleaning...")

        #inspect_df(df, name='Amazon sales')
        columns_to_drop = ['Unnamed: 22', 'promotion-ids', 'fulfilled-by', 'Style', 'currency', 'index']

        to_standardize = ['Status', 'Courier Status', 'Fulfilment', 'B2B', 'ship-state', 'ship-city']

        critical_columns = ['order_id', 'amount', 'date', 'quantity', 'status', 'fulfillment']
        rename_map = {'Fulfilment': 'fulfillment', 'Qty': 'quantity'}

        existing_drop_cols = [col for col in columns_to_drop if col in df.columns]
        try:
            df = df.drop(columns=existing_drop_cols )
            logger.debug(f"Dropped columns: {existing_drop_cols }")
        except Exception as e:
            logger.error(f"Error dropping columns: {str(e)}")

        # standardize specified columns
        try:
            for col in to_standardize:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.lower().str.strip()
                    logger.debug(f"Standardized column: {col}")
                    
            rename_cols = {k: v for k, v in rename_map.items() if k in df.columns}
            if rename_cols:
                df.rename(columns=rename_cols, inplace=True)
                logger.debug(f"Renamed columns: {rename_cols}'")
        except Exception as e:
            logger.error(f"Error standardizing columns: {str(e)}")
            raise
        
        # Initial cleaning
        try:
            df = df.drop_duplicates().reset_index(drop=True)
            df = df[df.isna().mean(axis=1) < 0.5]
            logger.debug(f"Shape after initial cleaning: {df.shape}")
        except Exception as e:
            logger.error(f"Error in initial cleaning: {str(e)}")

        # Transform data
        try:
            df_clean = transform(df)
            logger.info("Data transformation completed")
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
        
        # Critical columns check - only check columns that exist
        try:
            existing_critical_columns = [col for col in critical_columns if col in df_clean.columns]
            df_clean = df_clean.dropna(subset=existing_critical_columns)
            logger.debug(f"Shape after dropping rows with missing critical columns: {df_clean.shape}")
        except Exception as e:
            logger.error(f"Error checking critical columns: {str(e)}")

        # Check for conflicting order IDs
        flagged_duplicates = pd.DataFrame()
        if 'order_id' in df_clean.columns:
            try:
                is_duplicate = df_clean.duplicated(subset=['order_id'], keep=False)
                conflicting_orders = df_clean.loc[is_duplicate, 'order_id'].unique()
                
                if len(conflicting_orders) > 0:
                    dup_mask = is_duplicate.copy()
                    flagged_duplicates = df_clean[dup_mask]

                    df_clean = df_clean[~dup_mask]
                    logger.debug(f"Shape after removing conflicting order IDs: {df_clean.shape}")

            except Exception as e:
                logger.error(f"Error processing order IDs: {str(e)}")

        logger.info("Amazon Sale Report cleaned successfully.")
        return df_clean, flagged_duplicates
    
    except Exception as e:
        logger.error(f"Error in clean_amazon_sale: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    
def clean_sale(df):
    try:
        logger.info("Starting sale data cleaning...")
        #inspect_df(df, name='sale')
        try:
            if 'index' in df.columns:
                df = df.drop(columns=['index'])
                logger.debug("Dropped 'index' column")
        except Exception as e:
            logger.error(f"Error dropping index column: {str(e)}")

        # Initial cleaning
        try:
            df = df.drop_duplicates().reset_index(drop=True)
            df = df[df.isna().mean(axis=1) < 0.5]
            logger.debug(f"Shape after initial cleaning: {df.shape}")
        except Exception as e:
            logger.error(f"Error in initial cleaning: {str(e)}")

        try:    
            df_clean = transform(df)
            logger.info("Sale data transformation completed")
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
        try:
            if 'design_no.' in df_clean.columns:
                df_clean = df_clean.rename(columns = {'design_no.':'design_no'})
                logger.debug("Renamed 'design_no.' to 'design_no'")
        except Exception as e:
            logger.error(f"Error renaming column: {str(e)}")
        
        logger.info("Sale data cleaned successfully.")
        return df_clean

    except Exception as e:
        logger.error(f"Error cleaning Sale data: {e}")
        return pd.DataFrame()
    
def clean_international_sale(df):
    try:
        logger.info("Starting international sales data cleaning...")
        #inspect_df(df, name='international sale')

        # Initial cleaning
        try:
            df = df.drop_duplicates().reset_index(drop=True)
            df = df[df.isna().mean(axis=1) < 0.5]
            logger.debug(f"Shape after initial cleaning: {df.shape}")
        except Exception as e:
            logger.error(f"Error in initial cleaning: {str(e)}")

        try:
            if 'index' in df.columns:
                df = df.drop(columns=['index'])
                logger.debug("Dropped 'index' column")
        except Exception as e:
            logger.error(f"Error dropping index column: {str(e)}")

        try:
            if 'GROSS AMT' in df.columns:
                df = df.rename(columns = {'GROSS AMT':'gross_amount'})
                logger.debug("Renamed 'GROSS AMT.' to 'gross_amount'")
        except Exception as e:
            logger.error(f"Error renaming column: {str(e)}")

        try:    
            all_rows = df.values.tolist()
            original_columns = df.columns.tolist()

            header_1 = [original_columns]
            header_2 = []

            found_header = False
            
            for i, row in enumerate(all_rows):
                if is_all_strings(row):
                    print(f"Row {i+1} contains only string values with letters: {row}")
                    logger.debug(f"Found header row at position {i+1}: {row}")
                    header_1.extend(all_rows[:i])
                    header_2 = all_rows[i:] 
                    found_header = True
                    break
            else:
                header_1.extend(all_rows)

            international_sale_1 = pd.DataFrame(header_1[1:], columns=header_1[0])
            logger.debug(f"First part shape: {international_sale_1.shape}")
            logger.debug(f"First part sample:\n{international_sale_1.head()}")

            international_1 = transform(international_sale_1)
            international_1 = standardize_text_columns(international_1)
            
            international_sale_2 = pd.DataFrame()
            if found_header and len(header_2) > 1:
                international_sale_2 = pd.DataFrame(header_2[1:], columns=header_2[0])
                international_sale_2.columns = international_sale_2.columns.str.lower().str.strip().str.replace(r'[\s\-]+','_', regex=True)
                try:
                    if 'gross_amt' in df.columns:
                    df = df.rename(columns = {'gross_amt':'gross_amount'})
                    logger.debug("Renamed 'gross_amt.' to 'gross_amount'")
                except Exception as e:
                    logger.error(f"Error renaming column: {str(e)}")

                logger.debug(f"Second part shape: {international_sale_2.shape}")
                logger.debug(f"Second part sample:\n{international_sale_2.head()}")

                international_2 = transform(international_sale_2)
                international_2 = standardize_text_columns(international_2)

            if len(header_2) > 1:
                logger.info("International Sale Report data split and cleaned successfully.")
            else:
                logger.info("International Sale Report cleaned successfully.")

            return international_1, international_2

        except Exception as e:
            logger.error(f"Error processing international sales data: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error cleaning International sale data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def load_to_pg(df_amazon, df_amazon_2, df_sale, df_international_1, df_international_2, schema_name="kaggle"):
    conn = None
    cur = None
    try:
        logger.info("Starting PostgreSQL data loading process")

        logger.debug("Validating input dataframes")
        if df_amazon.empty and df_amazon_2.empty and df_sale.empty and df_international_1.empty and df_international_2.empty:
            logger.warning("All input DataFrames are empty - nothing to load")
            return False
        
        logger.info("Attempting to connect to PostgreSQL...")
        try:
            conn = psycopg2.connect(
                dbname = postgres_db,
                user = postgres_user,
                password = postgres_pass,
                host = postgres_host,
                port = 5432,
                options=f"-c search_path={schema_name},public" 
            )
            cur = conn.cursor()
            logger.info(f"Successfully connected to PostgreSQL (schema: {schema_name})")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.amazon_sale(
            order_id TEXT NOT NULL,
            date DATE NOT NULL,
            status TEXT,
            fulfillment TEXT,
            sales_channel TEXT,
            ship_service_level TEXT,
            sku TEXT,
            category TEXT,
            size TEXT,
            asin TEXT,
            courier_status TEXT,
            quantity INTEGER,
            amount FLOAT,
            ship_city TEXT,
            ship_state TEXT,
            ship_postal_code FLOAT,
            ship_country TEXT,
            b2b TEXT,
            loaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, date)
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.amazon_sale_version(
            version_id SERIAL PRIMARY KEY,
            order_id TEXT NOT NULL,
            date DATE NOT NULL,
            status TEXT,
            fulfillment TEXT,
            sales_channel TEXT,
            ship_service_level TEXT,
            sku TEXT,
            category TEXT,
            size TEXT,
            asin TEXT,
            courier_status TEXT,
            quantity INTEGER,
            amount FLOAT,
            ship_city TEXT,
            ship_state TEXT,
            ship_postal_code FLOAT,
            ship_country TEXT,
            b2b TEXT,
            loaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.sale(
            id SERIAL PRIMARY KEY,
            sku_code TEXT NOT NULL,
            design_no TEXT,
            stock INTEGER,
            category TEXT,
            size TEXT,
            color TEXT,
            loaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.international_sales(
            id SERIAL PRIMARY KEY,
            data_source TEXT CHECK (data_source IN ('part1', 'part2')),
            customer TEXT,
            date DATE,
            months TEXT,
            style TEXT,
            sku TEXT,
            pcs INTEGER,
            rate TEXT,
            gross_amount FLOAT,
            size TEXT NULL,
            stock INTEGER NULL,
            loaded_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        load_time = datetime.now()

        if not df_amazon.empty:
            df_amazon['loaded_at'] = load_time
            columns = df_amazon.columns.tolist()
            values = [tuple(row) for row in df_amazon[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO {schema_name}.amazon_sale({','.join(columns)})
                VALUES %s
                ON CONFLICT (order_id, date) DO NOTHING;
                """,
                values
            )
        logger.info("Amazon main data inserted.")

        if not df_amazon_2.empty:
            df_amazon_2['loaded_at'] = load_time
            columns = df_amazon_2.columns.tolist()
            values = [tuple(row) for row in df_amazon_2[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO {schema_name}.amazon_sale_version({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        logger.info("Amazon version data inserted.")

        if not df_sale.empty:
            df_sale['loaded_at'] = load_time
            columns = df_sale.columns.tolist()

            values = [tuple(row) for row in df_sale[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO {schema_name}.sale ({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        logger.info("Sale data inserted.")

        if not df_international_1.empty:
            df_international_1['data_source'] = 'part1'
            df_international_1['loaded_at'] = load_time
            table_cols = ['customer', 'date', 'months', 'style', 'sku', 'pcs', 'rate', 'gross_amount', 'size', 'stock']
            for col in table_cols:
                if col not in df_international_1.columns:
                    df_international_1[col] = None
            df_international_1 = df_international_1[table_cols + ['data_source'] + ['loaded_at']]
            columns = df_international_1.columns.tolist()
            values = [tuple(row) for row in df_international_1[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO {schema_name}.international_sales ({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        logger.info("International sale part 1 data inserted.")

        if not df_international_2.empty:
            df_international_2['data_source'] = 'part2'
            df_international_2['loaded_at'] = load_time
            table_cols = ['customer', 'date', 'months', 'style', 'sku', 'pcs', 'rate', 'gross_amount', 'size', 'stock']
            for col in table_cols:
                if col not in df_international_2.columns:
                    df_international_2[col] = None
            df_international_2 = df_international_2[table_cols + ['data_source']+['loaded_at']]
            columns = df_international_2.columns.tolist()
            values = [tuple(row) for row in df_international_2[columns].to_numpy()]
            execute_values(
                cur,
                f"""
                INSERT INTO {schema_name}.international_sales ({','.join(columns)})
                VALUES %s;
                """,
                values
            )
        logger.info("International sale part 2 data inserted.")
        
        conn.commit()
        logger.info("All data successfully loaded to PostgreSQL.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error loading to PostgreSQL: {e}")
        logger.error(f"Error loading to PostgreSQL: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("PostgreSQL connection closed.")

def load_to_s3(dataframes: dict, s3_bucket):
    success_count = 0
    try:
        logger.info("Starting S3 upload process")
        if not s3_bucket:
            logger.error("Missing required environment variable: S3 BUCKET")
            raise

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        seen_ids = set()

        for name, df in dataframes.items():
            try:
                if df.empty:
                    logger.warning(f"Skipping '{name}' because DataFrame is empty")
                    continue
                if id(df) in seen_ids:
                    logger.warning(f"Skipping '{name}' because it is a duplicate DataFrame")
                    continue

                seen_ids.add(id(df))
                logger.info(f"Processing DataFrame: {name}")
                s3_key = f"kaggle/cleaned/{name}_{timestamp}.csv"

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)

                try:
                    s3.put_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Body=csv_buffer.getvalue()
                    )
                    success_count += 1
                    logger.info(f"Successfully uploaded '{name}' to s3://{s3_bucket}/{s3_key}")

                except Exception as e:
                    logger.error(f"Failed to upload {name} to S3: {str(e)}")
                    continue

            except Exception as e:
                logger.error(f"Error processing {name}: {str(e)}")
        if success_count > 0:
            logger.info("Cleaned files uploaded to S3 successfully.")
        else:
            logger.warning("No files uploaded to S3.")
    except Exception as e:
        logger.error(f"Uploading to S3 failed: {e}")
        raise

def lambda_handler(event, context):
    try:
        logger.info("Starting Kaggle ETL Lambda process...")
        logger.info("Fetching recent S3 files (within last 1 minutes)...")
        recent_files = get_latest_sale_files(minutes=1)

        cleaned_amazon = cleaned_amazon_dup = cleaned_sale = cleaned_international_1 = cleaned_international_2 = None

        if not recent_files:
            logger.warning("No recent S3 files found to process.")
            return {
                "statusCode": 204,
                "body": "No recent files found in S3."
            }
        
        logger.info(f"Found {len(recent_files)} recent files. Beginning classification...")

        for file in recent_files:
            key = file['key'].lower()
            df = file['df']
            logger.info(f"Processing file: {key}")

            if "amazon" in key and 'sale' in key:
                logger.info(f"Classified as Amazon Sale Report: {key}")
                cleaned_amazon, cleaned_amazon_dup = clean_amazon_sale(df)

            elif "international" in key and 'sale' in key:
                logger.info(f"Classified as International Sale Report: {key}")
                cleaned_international_1, cleaned_international_2 = clean_international_sale(df)

            elif "sale" in key and "international" not in key and 'amazon' not in key:
                logger.info(f"Classified as Regular Sale Report: {key}")
                cleaned_sale = clean_sale(df)

            else:
                logger.warning(f"Unrecognized file: {key} — skipping.")

        logger.info("Uploading cleaned data to S3...")
        upload_dict_raw = {
            "amazon_sale_cleaned": cleaned_amazon,
            "amazon_sale_duplicates": cleaned_amazon_dup,
            "sale_cleaned": cleaned_sale,
            "international_sale_part1": cleaned_international_1,
            "international_sale_part2": cleaned_international_2
        }
        upload_dict = {k: v for k, v in upload_dict_raw.items() if v is not None}

        load_to_s3(upload_dict, s3_bucket)

        logger.info("Loading cleaned data to PostgreSQL...")
        load_to_pg(cleaned_amazon, cleaned_amazon_dup, cleaned_sale, cleaned_international_1, cleaned_international_2)
        
        logger.info("ETL pipeline completed successfully.")

        return {
            "statusCode": 200,
            "body": "YouTube ETL completed successfully"
        }
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        return {
            "statusCode": 500,
            "body": f"ETL failed: {str(e)}"
        }