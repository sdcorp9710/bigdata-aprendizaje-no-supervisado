# ------------------------------------------
# K-MODES categórico (COVID19 México) con R local + Sparklyr, muestreado desde Spark 3.5
# ------------------------------------------
# - Columnas categóricas: DIABETES, HIPERTENSION, OBESIDAD, SEXO
# - Limpieza/codificación robusta de valores (1/2/97/98/99/SI/NO, etc.)
# - Lectura por chunks + muestreo para CSV grande
# - Selección de k por curva de codo (costo de K-modes)
# - Perfiles de clúster + exportación de asignaciones y gráficas

# Autor: (Luis Alejandro Santana Valadez)

suppressPackageStartupMessages({
  library(sparklyr)
  library(dplyr)
  library(DBI)
  library(ggplot2)
  library(sparklyr.nested)   # para separar vectores (PCA/features) -> pc1/pc2
})

# ---- Configuración -------------------------------------------
INPUT_CSV <- "file://C:/LASV/DOCTORADO/SEP-DIC 25/Big Data/Unidad 2/CSV/COVID19MEXICO.csv"      
OUT_DIR   <- "file://C:/LASV/DOCTORADO/SEP-DIC 25/Big Data/Unidad 2/OUT/bigdata_categorical"    
dir.create(gsub("^file://","", OUT_DIR), showWarnings = FALSE, recursive = TRUE)

config <- spark_config()
config[["sparklyr.shell.driver-memory"]] <- "8G"
config[["spark.sql.shuffle.partitions"]] <- 200
sc <- spark_connect(master = "local[*]", config = config)

# ---- Ingesta -------------------------------------------------
df_raw <- spark_read_csv(
  sc, name = "df_raw_cat", path = INPUT_CSV,
  header = TRUE, infer_schema = TRUE, memory = FALSE, delimiter = ","
)

# ---- Selección de categóricas --------------------------------
cat_cols <- c(
  "SEXO","DIABETES","HIPERTENSION","OBESIDAD"
)
df_cat <- df_raw %>% select(all_of(cat_cols))

# ---- Muestreo representativo (proporcional) ------------------
target_n <- 100000  # 50k–200k según memoria local
n_total  <- sdf_nrow(df_cat)
frac     <- min(1, target_n / n_total)

set.seed(2025)
df_sample <- df_cat %>% sdf_sample(fraction = frac, replacement = FALSE, seed = 2025)

# ---- Colecta a R como factores --------------------------------
# Convertir a character en Spark, luego factor en R mantiene 97/98/99 como niveles
df_sample_chr <- df_sample %>% mutate_at(vars(all_of(cat_cols)), as.character)
local_df <- df_sample_chr %>% collect()
for(v in cat_cols) local_df[[v]] <- as.factor(local_df[[v]])

# ---- Selección de K por costo (withindiff) --------------------
set.seed(123)
ks <- 2:12
costs <- numeric(length(ks))
for (i in seq_along(ks)) {
  km <- kmodes(local_df[cat_cols], modes = ks[i], iter.max = 50, weighted = FALSE)
  costs[i] <- km$withindiff
}
cost_df <- data.frame(k = ks, cost = costs)

png(filename = file.path(gsub("^file://","", OUT_DIR), "kmodes_elbow.png"), width = 900, height = 600)
plot(cost_df$k, cost_df$cost, type = "b", xlab = "k", ylab = "Costo (withindiff)", main = "Selección de k — K-modes")
dev.off()

best_k <- cost_df$k[which.min(cost_df$cost)]
# best_k <- 6  # opción manual, pero obtiene un costo cercano al mínimo automático

# ---- Entrenamiento final -------------------------------------
set.seed(321)
km_final <- kmodes(local_df[cat_cols], modes = best_k, iter.max = 100, weighted = FALSE)
local_df$cluster <- factor(km_final$cluster)

# ---- Perfiles por clúster ------------------------------------
# Guardar tablas de proporciones por variable
for (v in cat_cols) {
  tab <- as.data.frame(prop.table(table(local_df$cluster, local_df[[v]]), 1))
  colnames(tab) <- c("cluster", v, "prop")
  write.csv(tab, file = file.path(gsub("^file://","", OUT_DIR), paste0("perfil_", v, ".csv")), row.names = FALSE)
}

# ---- Gráficas (top-10 niveles por variable) ------------------
for (v in cat_cols) {
  tab <- as.data.frame(prop.table(table(local_df$cluster, local_df[[v]]), 1))
  colnames(tab) <- c("cluster", "nivel", "prop")
  top_levels <- tab %>%
    dplyr::group_by(nivel) %>%
    dplyr::summarise(m = max(prop)) %>%
    dplyr::arrange(desc(m)) %>% head(10) %>% dplyr::pull(nivel)
  tab2 <- tab %>% dplyr::filter(nivel %in% top_levels)
  p <- ggplot(tab2, aes(x = nivel, y = prop, fill = cluster)) +
    geom_col(position = "dodge") +
    labs(title = paste("Distribución por clúster —", v), x = v, y = "Proporción") +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 30, hjust = 1))
  ggsave(filename = file.path(gsub("^file://","", OUT_DIR), paste0("stacked_", v, ".png")), plot = p, width = 10, height = 6)
}

# ---- Guardado de resultados ----------------------------------
write.csv(data.frame(k = best_k, costo_minimo = min(cost_df$cost)),
          file = file.path(gsub("^file://","", OUT_DIR), "kmodes_resumen.csv"), row.names = FALSE)

saveRDS(km_final, file = file.path(gsub("^file://","", OUT_DIR), "kmodes_model.rds"))
write.csv(local_df[, c(cat_cols, "cluster")],
          file = file.path(gsub("^file://","", OUT_DIR), "kmodes_clusters_asignacion.csv"),
          row.names = FALSE)

cat("✅ Listo (categórico): resultados en ", OUT_DIR, "\n")
spark_disconnect(sc)
