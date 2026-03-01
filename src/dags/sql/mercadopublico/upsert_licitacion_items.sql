INSERT INTO mercadopublico_licitacion_items (
    correlativo,
    codigo_licitacion,
    codigo_producto,
    codigo_categoria,
    categoria,
    nombre_producto,
    descripcion,
    unidad_medida,
    cantidad,
    rut_proveedor,
    nombre_proveedor,
    cantidad_adjudicada,
    monto_unitario,
    fecha_proceso
)
VALUES (
    %(correlativo)s,
    %(codigo_licitacion)s,
    %(codigo_producto)s,
    %(codigo_categoria)s,
    %(categoria)s,
    %(nombre_producto)s,
    %(descripcion)s,
    %(unidad_medida)s,
    %(cantidad)s,
    %(rut_proveedor)s,
    %(nombre_proveedor)s,
    %(cantidad_adjudicada)s,
    %(monto_unitario)s,
    NOW()
)
ON CONFLICT (correlativo, codigo_licitacion, codigo_producto) DO UPDATE SET
    codigo_categoria = EXCLUDED.codigo_categoria,
    categoria = EXCLUDED.categoria,
    nombre_producto = EXCLUDED.nombre_producto,
    descripcion = EXCLUDED.descripcion,
    unidad_medida = EXCLUDED.unidad_medida,
    cantidad = EXCLUDED.cantidad,
    rut_proveedor = EXCLUDED.rut_proveedor,
    nombre_proveedor = EXCLUDED.nombre_proveedor,
    cantidad_adjudicada = EXCLUDED.cantidad_adjudicada,
    monto_unitario = EXCLUDED.monto_unitario,
    fecha_proceso = NOW()