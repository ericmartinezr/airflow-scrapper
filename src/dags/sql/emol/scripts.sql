CREATE TABLE IF NOT EXISTS emol (
    id integer primary key,
    categoria TEXT,
    titulo text,
    bajada text,
    noticia text,
    fecha_publicacion timestamp,
    fecha_modificacion timestamp,
    fecha_proceso timestamp
)