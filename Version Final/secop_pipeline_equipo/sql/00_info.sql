-- Este archivo es solo de referencia para DBeaver.
-- Las tablas se crean y cargan desde Python.
SELECT current_database(), current_user;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('secop', 'gold')
ORDER BY table_schema, table_name;
