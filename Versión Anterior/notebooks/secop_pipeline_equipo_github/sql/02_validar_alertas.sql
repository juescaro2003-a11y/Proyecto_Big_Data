-- Parámetros SQLAlchemy :cut y :version. Ejecutar con los valores de la evaluación,
-- no con MAX(fecha_corte), para no confundir otra versión o corte concurrente.
SELECT id_contrato,codigo_entidad,codigo_proveedor,valor_contrato,
       participacion_proveedor,d1_concentracion,zscore_robusto,d2_valor_atipico,
       participacion_diciembre,umbral_d3,alerta_d3,score_total,nivel_alerta,
       alerta_binaria,clasificacion
FROM analytics.alertas_contratacion_directa
WHERE fecha_corte=CAST(:cut AS date) AND version_modelo=:version
ORDER BY id_contrato LIMIT 10;

SELECT clasificacion,COUNT(*) AS total_contratos,AVG(score_total) AS score_promedio
FROM analytics.alertas_contratacion_directa
WHERE fecha_corte=CAST(:cut AS date) AND version_modelo=:version
GROUP BY clasificacion ORDER BY total_contratos DESC;

SELECT COUNT(*) FILTER(WHERE d1_concentracion=1) AS alertas_concentracion,
       COUNT(*) FILTER(WHERE d2_valor_atipico=1) AS alertas_valor_atipico,
       COUNT(*) FILTER(WHERE alerta_d3=1) AS alertas_acumulacion_diciembre
FROM analytics.alertas_contratacion_directa
WHERE fecha_corte=CAST(:cut AS date) AND version_modelo=:version;

SELECT id_contrato,fecha_corte,version_modelo,COUNT(*)
FROM analytics.alertas_contratacion_directa
GROUP BY id_contrato,fecha_corte,version_modelo HAVING COUNT(*)>1;
