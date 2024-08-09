
CREATE OR REPLACE TABLE DOCDB.DOCSCHEMA.DOC_SPLIT_TBL (
    YEAR VARCHAR,
    QUARTER VARCHAR,
    FUND_NAME VARCHAR,
    FILE_NAME VARCHAR,
    FILE_SIZE VARCHAR,
    LAST_MODIFIED VARCHAR,
    SNOWFLAKE_FILE_URL VARCHAR,
    ocrScore FLOAT,
    DealName_score FLOAT,
    DealName_value STRING,
    FundName_score FLOAT,
    FundName_value STRING,
    Fund_names_score FLOAT,
    Fund_names_value STRING,
    Valuationamount_score FLOAT,
    Valuationamount_value STRING,
    Valuationdate_score FLOAT,
    Valuationdate_value STRING
);

CREATE OR REPLACE PROCEDURE DOC_SPLIT_AND_INSERT_DATA_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'split_and_insert_data'
AS
$$
import json
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session

def split_and_insert_data(session: Session) -> str:
    # Fetch data from the RAW_TBL
    df = session.table('DOCDB.DOCSCHEMA.RAW_TBL')
    
    # Process each row
    processed_rows = []
    for row in df.collect():
        file_name = row['FILE_NAME']
        file_size = row['FILE_SIZE']
        last_modified = row['LAST_MODIFIED']
        snowflake_file_url = row['SNOWFLAKE_FILE_URL']
        doc_data_json = row['DOC_DATA_JSON']
        
        # Extract YEAR, QUARTER, and FUND_NAME from FILE_NAME
        file_name_parts = file_name.split('/')
        year_quarter = file_name_parts[0].split('Q')
        year = year_quarter[0]
        quarter = 'Q' + year_quarter[1]
        fund_name = file_name_parts[1]

        # Parse the DOC_DATA_JSON field
        doc_data = json.loads(doc_data_json)
        ocr_score = doc_data.get('__documentMetadata', {}).get('ocrScore')
        deal_name_score = doc_data.get('DealName', [{}])[0].get('score')
        deal_name_value = doc_data.get('DealName', [{}])[0].get('value')
        fund_name_score = doc_data.get('FundName', [{}])[0].get('score')
        fund_name_value = doc_data.get('FundName', [{}])[0].get('value')

        # Flatten the Fund_names JSON array
        for fund in doc_data.get('Fund_names', []):
            fund_names_score = fund.get('score')
            fund_names_value = fund.get('value')

            # Append processed row
            processed_rows.append((year, quarter, fund_name, file_name, file_size, last_modified, snowflake_file_url, ocr_score,
                                   deal_name_score, deal_name_value, fund_name_score, fund_name_value, fund_names_score, fund_names_value,
                                   doc_data.get('Valuationamount', [{}])[0].get('score'),
                                   doc_data.get('Valuationamount', [{}])[0].get('value'),
                                   doc_data.get('Valuationdate', [{}])[0].get('score'),
                                   doc_data.get('Valuationdate', [{}])[0].get('value')))
    
    # Create a DataFrame from processed rows
    processed_df = session.create_dataframe(processed_rows, schema=['YEAR', 'QUARTER', 'FUND_NAME', 'FILE_NAME', 'FILE_SIZE', 'LAST_MODIFIED', 
                                                                    'SNOWFLAKE_FILE_URL', 'ocrScore', 'DealName_score', 'DealName_value', 
                                                                    'FundName_score', 'FundName_value', 'Fund_names_score', 'Fund_names_value', 
                                                                    'Valuationamount_score', 'Valuationamount_value', 'Valuationdate_score', 
                                                                    'Valuationdate_value'])

    # Insert data into DOC_SPLIT_TBL
    processed_df.write.mode('append').save_as_table('DOCDB.DOCSCHEMA.DOC_SPLIT_TBL')
    
    return 'Procedure completed successfully.'
$$;

CALL DOC_SPLIT_AND_INSERT_DATA_PROC();


select * from DOC_SPLIT_TBL;
