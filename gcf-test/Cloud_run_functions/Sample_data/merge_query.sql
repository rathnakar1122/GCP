MERGE INTO `rathnakar-18m85a0320-hiscox.prod.CUSTOMER_INFO_f` tgt
USING `rathnakar-18m85a0320-hiscox.MAIN.CUSTOMER_INFO` src
ON tgt.cust_id = src.cust_id

WHEN MATCHED AND tgt.source_system_date < CURRENT_DATE() THEN
  UPDATE SET
    tgt.cust_name = src.cust_name,
    tgt.phone_number = src.phone_number,
    tgt.email_address = src.email_address,
    tgt.address = src.address,
    tgt.source_system_date = src.source_system_date,
    tgt.status = 'active'

WHEN NOT MATCHED THEN
  INSERT (
    cust_id, cust_name, phone_number, email_address, address, source_system_date, source_update_date, start_date, end_date, status
  )
  VALUES (
    src.cust_id, src.cust_name, src.phone_number, src.email_address, src.address, src.source_system_date, 
    src.source_system_date, DATE '2025-01-16', NULL, 'active'
  );
