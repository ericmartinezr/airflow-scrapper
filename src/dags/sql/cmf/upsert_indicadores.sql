INSERT INTO cmf_indicadores (
    indicador,
    valor,
    fecha_valor,
    fecha_proceso
)
VALUES (
    %(indicador)s,
    %(valor)s,
    %(fecha_valor)s,
    NOW()
)
ON CONFLICT (indicador, fecha_valor) DO UPDATE SET
    valor = EXCLUDED.valor,
    fecha_proceso = NOW()