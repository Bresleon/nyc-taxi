SELECT
    t.*,
    -- Enriquecimiento Role-Playing con zonas
    pz.borough AS pu_borough,
    pz.zone_name AS pu_zone,
    dz.borough AS do_borough,
    dz.zone_name AS do_zone,

    -- Decodificación de negocio del medio de pago
    CASE t.payment_type
        WHEN 0 THEN '0 - Flex Fare / App'
        WHEN 1 THEN '1 - Credit Card'
        WHEN 2 THEN '2 - Cash (Efectivo)'
        WHEN 3 THEN '3 - No Charge'
        WHEN 4 THEN '4 - Dispute'
        WHEN 5 THEN '5 - Unknown'
        WHEN 6 THEN '6 - Voided Trip'
        ELSE 'Otro'
    END AS payment_type_desc,

    -- Clasificación de viajes aeroportuarios (Pregunta 5)
    CASE
        WHEN pz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
         AND dz.zone_name IN ('JFK Airport', 'LaGuardia Airport')
            THEN 'Inter-Aeropuerto (JFK <-> LGA)'
        WHEN pz.zone_name = 'JFK Airport' OR dz.zone_name = 'JFK Airport'
            THEN 'JFK Airport'
        WHEN pz.zone_name = 'LaGuardia Airport' OR dz.zone_name = 'LaGuardia Airport'
            THEN 'LaGuardia Airport'
        ELSE NULL
    END AS airport_category,

    -- Indicador booleano de cargo por congestión (Pregunta 3)
    CASE WHEN t.cbd_congestion_fee > 0 THEN 1 ELSE 0 END AS is_cbd_congestion_trip

FROM {{ ref('stg_tlc__yellow_trips') }} t
LEFT JOIN {{ ref('stg_tlc__taxi_zones') }} pz ON t.pu_location_id = pz.location_id
LEFT JOIN {{ ref('stg_tlc__taxi_zones') }} dz ON t.do_location_id = dz.location_id
