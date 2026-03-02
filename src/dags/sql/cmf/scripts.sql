CREATE TABLE IF NOT EXISTS cmf_indicadores (
    indicador TEXT,
    valor NUMERIC,
    fecha_valor DATE,
    fecha_proceso TIMESTAMP,
    PRIMARY KEY (indicador, fecha_valor)
)
