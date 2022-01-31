CREATE TABLE IF NOT EXISTS commercial.odbc_intage
(
    Channel String,
    Category String,
    Product_JP String,
    SKU String,
    SKU_Code String,
    Maker String,
    Kubun String,
    Package_Size String,
    Region String,
    Week String,
    Values_Liters Int32,
    Values_Unit_Case Int32,
    Values_JPY Int32,
    Values_Turn Int32,
    Values_Coverage Int32,
    Values_Bottles Int32
)
ENGINE = ODBC("DSN=MarkLogicSQL_Commercial", '', "INTAGE_All")