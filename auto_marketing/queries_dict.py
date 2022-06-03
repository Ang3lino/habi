names_queries = {
    "cierres_col": 
        """
        SELECT t0.ciudad_id, t0.loc_inmueble_id, t0.label, t0.name, t0.pais_id FROM (
            SELECT 
                t5.id AS ciudad_id, 
                t4.id AS loc_inmueble_id, 
                t5.label, 
                t5.name, 
                t5.pais_id 
            FROM `papyrus-data.habi_wh_bi.Consolidado_BI` as t1
            LEFT JOIN `papyrus-data.habi_wh.tabla_negocio_inmueble` as t2
                on t1.nid = t2.nid
            LEFT JOIN `papyrus-data.habi_wh.tabla_inmueble_v2` as t3
                on t2.inmueble_id = t3.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_localizacion_inmueble_v2` as t4
                on t4.id = t3.localizacion_new_id
            LEFT JOIN `papyrus-data.habi_wh.tabla_ciudad` as t5
                on t4.ciudad_id = t5.id
            WHERE t1.v_fecha_promesa IS NOT NULL
        ) AS t0;""",
    "calificados_fuente_col": 
        """
        SELECT distinct((t2.correo)) as email, t2.telefono as phone, t2.nombre_o_inmobiliaria as fn, t5.label as ct
            FROM `papyrus-data.habi_wh.tabla_negocio_inmueble` as t1
            LEFT JOIN `papyrus-data.habi_wh.tabla_contacto_v2` as t2
                on t1.contacto_id = t2.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_inmueble_v2` as t3
                on t3.id = t1.inmueble_id
            LEFT JOIN `papyrus-data.habi_wh.tabla_localizacion_inmueble_v2` as t4
                on t3.localizacion_new_id = t4.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_ciudad` as t5
                on t4.ciudad_id = t5.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_historico_estado_v2` as t6
                on t6.negocio_id = t1.id
            LEFT JOIN `papyrus-data.habi_wh_bi.consolidado_bi_v2` as t7
                on t7.nid = t1.nid
            WHERE t1.fecha_creacion > '2021-04-01' and t2.correo is not null;
        """,
    "calificados_totales_col":
        """
        SELECT DISTINCT (t2.correo) as email, t2.telefono as phone, t2.nombre_o_inmobiliaria as fn, t5.label as ct
            FROM `papyrus-data.habi_wh.tabla_negocio_inmueble` as t1
            LEFT JOIN `papyrus-data.habi_wh.tabla_contacto_v2` as t2
            on t1.contacto_id = t2.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_inmueble_v2` as t3
            on t3.id = t1.inmueble_id
            LEFT JOIN `papyrus-data.habi_wh.tabla_localizacion_inmueble_v2` as t4
            on t3.localizacion_new_id = t4.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_ciudad` as t5
            on t4.ciudad_id = t5.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_historico_estado_v2` as t6
            on t6.negocio_id = t1.id
            LEFT JOIN `papyrus-data.habi_wh_bi.consolidado_bi_v2` as t7
            on t7.nid = t1.nid
            WHERE t3.fuente_id = 1 and t1.fecha_creacion > "2021-08-01" and t2.telefono is not null and t2.correo is not null;
        """,
    "fuentes_web_col": 
        """
        SELECT DISTINCT (t2.correo) as email, t2.telefono as phone, t2.nombre_o_inmobiliaria as fn, t5.label as ct
            FROM `papyrus-data.habi_wh.tabla_negocio_inmueble` as t1
            LEFT JOIN `papyrus-data.habi_wh.tabla_contacto_v2` as t2
            on t1.contacto_id = t2.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_inmueble_v2` as t3
            on t3.id = t1.inmueble_id
            LEFT JOIN `papyrus-data.habi_wh.tabla_localizacion_inmueble_v2` as t4
            on t3.localizacion_new_id = t4.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_ciudad` as t5
            on t4.ciudad_id = t5.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_historico_estado_v2` as t6
            on t6.negocio_id = t1.id
        WHERE t3.fuente_id in (31,3) and t1.fecha_creacion > "2021-04-01" and t2.telefono is not null and t2.correo is not null;
        """,
    "ventanas_col":
        """
        SELECT DISTINCT (t2.correo) as email, t2.telefono as phone, t2.nombre_o_inmobiliaria as fn, t5.label as ct
            FROM `papyrus-data.habi_wh.tabla_negocio_inmueble` as t1
            LEFT JOIN `papyrus-data.habi_wh.tabla_contacto_v2` as t2
            on t1.contacto_id = t2.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_inmueble_v2` as t3
            on t3.id = t1.inmueble_id
            LEFT JOIN `papyrus-data.habi_wh.tabla_localizacion_inmueble_v2` as t4
            on t3.localizacion_new_id = t4.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_ciudad` as t5
            on t4.ciudad_id = t5.id
            LEFT JOIN `papyrus-data.habi_wh.tabla_historico_estado_v2` as t6
            on t6.negocio_id = t1.id
            WHERE t3.fuente_id = 1 and t1.fecha_creacion > "2021-04-01" and t2.telefono is not null and t2.correo is not null;
        """,
    "ventanas_mx":
        """
            select DISTINCT (t2.email), t2.phone, t2.name_real_estate as fn, t4.zip_code as zip, t5.label as ct
                from `papyrus-data-mx.habi_wh.property_deal` as t1
                LEFT JOIN `papyrus-data-mx.habi_wh.contact` as t2 
                on t1.contact_id = t2.id
                LEFT JOIN `papyrus-data-mx.habi_wh.property` as t3
                on t3.id = t1.property_id
                LEFT JOIN `papyrus-data-mx.habi_wh.property_location` as t4
                on t4.id = t3.property_location_id
                LEFT JOIN `papyrus-data-mx.habi_wh.city` as t5
                on t4.city_id = t5.id
                LEFT JOIN `papyrus-data-mx.habi_wh.history_state` as t6
                on t6.deal_id = t1.id
                WHERE t3.source_id = 1 and t3.date_create >= '2021-04-01';
        """,
    "calificados_fuente_web_mx":
        """
        select DISTINCT (t2.email), t2.phone, t2.name_real_estate as fn, t4.zip_code as zip, t5.label as ct
            from `papyrus-data-mx.habi_wh.property_deal` as t1
            LEFT JOIN `papyrus-data-mx.habi_wh.contact` as t2 
            on t1.contact_id = t2.id
            LEFT JOIN `papyrus-data-mx.habi_wh.property` as t3
            on t3.id = t1.property_id
            LEFT JOIN `papyrus-data-mx.habi_wh.property_location` as t4
            on t4.id = t3.property_location_id
            LEFT JOIN `papyrus-data-mx.habi_wh.city` as t5
            on t4.city_id = t5.id
            LEFT JOIN `papyrus-data-mx.habi_wh.history_state` as t6
            on t6.deal_id = t1.id
            join `papyrus-data-mx.habi_wh_bi.Consolidado_BI_mx_` as t7
            on t7.nid = t1.nid
            WHERE t3.source_id in (31,3) and t1.date_create > "2021-04-01" and t2.phone is not null and t2.email is not null;
        """,
    "calificados_todas_las_fuentes_mx":
        """
        select DISTINCT (t2.email), t2.phone, t2.name_real_estate as fn, t4.zip_code as zip, t5.label as ct
            from `papyrus-data-mx.habi_wh.property_deal` as t1
            LEFT JOIN `papyrus-data-mx.habi_wh.contact` as t2 
            on t1.contact_id = t2.id
            LEFT JOIN `papyrus-data-mx.habi_wh.property` as t3
            on t3.id = t1.property_id
            LEFT JOIN `papyrus-data-mx.habi_wh.property_location` as t4
            on t4.id = t3.property_location_id
            LEFT JOIN `papyrus-data-mx.habi_wh.city` as t5
            on t4.city_id = t5.id
            LEFT JOIN `papyrus-data-mx.habi_wh.history_state` as t6
            on t6.deal_id = t1.id
            join `papyrus-data-mx.habi_wh_bi.Consolidado_BI_mx_` as t7
            on t7.nid = t1.nid
            WHERE t3.source_id in (31,3) and t1.date_create > "2021-04-01" and t2.phone is not null and t2.email is not null;
        """,
    "fuente_web_mx":
        """
        select DISTINCT (t2.email), t2.phone, t2.name_real_estate as fn, t4.zip_code as zip, t5.label as ct
            from `papyrus-data-mx.habi_wh.property_deal` as t1
            LEFT JOIN `papyrus-data-mx.habi_wh.contact` as t2 
            on t1.contact_id = t2.id
            LEFT JOIN `papyrus-data-mx.habi_wh.property` as t3
            on t3.id = t1.property_id
            LEFT JOIN `papyrus-data-mx.habi_wh.property_location` as t4
            on t4.id = t3.property_location_id
            LEFT JOIN `papyrus-data-mx.habi_wh.city` as t5
            on t4.city_id = t5.id
            LEFT JOIN `papyrus-data-mx.habi_wh.history_state` as t6
            on t6.deal_id = t1.id
            WHERE t3.source_id in (31,3) and t3.date_create >= '2021-04-01';
        """,
    "cierres_mx":
        """
        select DISTINCT (t2.email), t2.phone, t2.name_real_estate as fn, t4.zip_code as zip, t5.label as ct
            from `papyrus-data-mx.habi_wh.property_deal` as t1
            LEFT JOIN `papyrus-data-mx.habi_wh.contact` as t2 
            on t1.contact_id = t2.id
            LEFT JOIN `papyrus-data-mx.habi_wh.property` as t3
            on t3.id = t1.property_id
            LEFT JOIN `papyrus-data-mx.habi_wh.property_location` as t4
            on t4.id = t3.property_location_id
            LEFT JOIN `papyrus-data-mx.habi_wh.city` as t5
            on t4.city_id = t5.id
            LEFT JOIN `papyrus-data-mx.habi_wh.history_state` as t6
            on t6.deal_id = t1.id
            LEFT JOIN `papyrus-data-mx.habi_wh_bi.Consolidado_BI_mx_` as t7
            on t7.nid = t1.nid
            WHERE t7.comercial_compra is not null;
    """,
}