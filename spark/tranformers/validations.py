import logging

logger = logging.getLogger(__name__)

def validate_required_columns(df):
    """
    Validate that all required columns exist in the DataFrame.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_columns = [
        'airline', 'source', 'destination', 
        'base_fare_bdt', 'tax_and_surcharge_bdt', 
        'total_fare_bdt', 'departure_date_and_time', 
        'arrival_date_and_time' 
    ]
    
    missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
    
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logger.error(error_msg)
        return df, False
    
    logger.info("All required columns present")
    return df, True


def clean_column_names(df):
    """Clean and standardize column names."""
    for old_col in df.columns:
        new_col = old_col.strip() \
            .replace(' ', '_') \
            .replace('(', '') \
            .replace(')', '') \
            .replace('&', 'and') \
            .lower()
        
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)
    
    return df


