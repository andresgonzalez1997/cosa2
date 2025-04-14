SELECT 
  product_number,
  formula_code,
  product_name,
  product_form, 
  unit_weight,
  pallet_quantity,
  stocking_status,
  min_order_quantity,
  days_lead_time,
  fob_or_dlv,
  price_change,
  list_price,
  full_pallet_price,
  half_load_full_pallet_price,
  full_load_full_pallet_price,
  full_load_best_price,
  plant_location,
  date_inserted,
  source
    
FROM 
    @schema.comp_price_horizontal_files
    
WHERE
    plant_location = "@location"
    AND date_inserted = "@effective_date"
    
