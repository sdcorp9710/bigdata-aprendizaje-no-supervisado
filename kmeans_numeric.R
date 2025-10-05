# ------------------------------------------
# K-MEANS numérico (COVID19 México) con R local + Sparklyr, muestreado desde Spark 3.5
# ------------------------------------------
# - Columnas categóricas: DIABETES, HIPERTENSION, OBESIDAD, SEXO
# - Limpieza/codificación robusta de valores (1/2/97/98/99/SI/NO, etc.)
# - Lectura por chunks + muestreo para CSV grande
# - Selección de k por curva de codo (costo de K-modes)
# - Perfiles de clúster + exportación de asignaciones y gráficas

# Autor: (Luis Alejandro Santana Valadez)

# Notas:
#  - Evita funciones SQL que cambian entre builds (vector_to_array, etc.)
#  - Para proyección 2D usa sparklyr.nested::sdf_separate_column
#  - Selección de K por silhouette (estable en Spark 3.5)
#  - En Windows, omite parquet si aparece NativeIO error

suppressPackageStartupMessages({
  library(sparklyr)
  library(dplyr)
  library(DBI)
  library(ggplot2)
  library(sparklyr.nested)   # para separar vectores (PCA/features) -> pc1/pc2
})

cat("[", Sys.time(), "] 01: inicio script\n", sep = ""); flush.console()

# ===================== CONFIGURACIÓN ==========================
# Ajusta estas rutas a tu entorno:
INPUT_CSV <- "file:///C:/LASV/DOCTORADO/SEP-DIC 25/Big Data/Unidad 2/CSV/COVID19MEXICO.csv"
OUT_DIR   <- "file:///C:/LASV/DOCTORADO/SEP-DIC 25/Big Data/Unidad 2/OUT/bigdata_numeric"

# Normaliza OUT_DIR para el FS de Windows
dir.create(gsub("^file://","", OUT_DIR), showWarnings = FALSE, recursive = TRUE)
OUT_DIR_FS <- sub("^file:/+", "", OUT_DIR)     # quita "file:///"
OUT_DIR_FS <- sub("^/+", "", OUT_DIR_FS)       # quita "/" inicial si aparece
OUT_DIR_FS <- normalizePath(OUT_DIR_FS, winslash = "/", mustWork = FALSE)
if (!dir.exists(OUT_DIR_FS)) dir.create(OUT_DIR_FS, recursive = TRUE, showWarnings = FALSE)
.path_out <- function(fname) file.path(OUT_DIR_FS, fname)

# Selector de modo:
MODO <- "A1"  # "A1" = general | "A2" = solo fallecidos

# Spark config
config <- spark_config()
config[["sparklyr.shell.driver-memory"]] <- "8G"
config[["spark.sql.shuffle.partitions"]] <- 200
# Parche Windows: evita librería nativa de Hadoop
config[["sparklyr.shell.conf"]] <- c(
  "spark.hadoop.io.nativeio.use.windows.nativeio=false",
  "spark.hadoop.fs.file.impl=org.apache.hadoop.fs.LocalFileSystem"
)

cat("[", Sys.time(), "] 02: antes de spark_connect()\n", sep = ""); flush.console()
sc <- spark_connect(master = "local[*]", config = config)
on.exit(try(spark_disconnect(sc), silent = TRUE), add = TRUE)
cat("[", Sys.time(), "] 03: después de spark_connect()\n", sep = ""); flush.console()

# ====================== INGESTA ===============================
cat("[", Sys.time(), "] 04: antes de spark_read_csv()\n", sep = ""); flush.console()

# Detectar encabezado y delimitador leyendo SOLO la 1a línea localmente
local_csv_path <- sub("^file:///","", INPUT_CSV)
stopifnot(file.exists(local_csv_path))
local_csv_path <- normalizePath(local_csv_path, winslash = "/", mustWork = TRUE)

header_line <- readLines(local_csv_path, n = 1, warn = FALSE, encoding = "latin1")
if (length(header_line) == 0 || is.na(header_line)) {
  header_line <- readLines(local_csv_path, n = 1, warn = FALSE)
}
delims <- c(
  ","   = length(strsplit(header_line, ",")[[1]]),
  ";"   = length(strsplit(header_line, ";")[[1]]),
  "|"   = length(strsplit(header_line, "\\|")[[1]]),
  "\\t" = length(strsplit(header_line, "\t")[[1]])
)
delim <- names(which.max(delims))
split_pat <- if (delim == "|") "\\|" else if (delim == "\\t") "\t" else delim

header_names <- strsplit(header_line, split_pat)[[1]]
header_names <- trimws(header_names)
header_names <- gsub('^"|"$', "", header_names)   # quita comillas al inicio/fin
header_names <- gsub('^\ufeff', "", header_names) # quita BOM si existiera

cat("[", Sys.time(), "] 04b: delimitador detectado = ",
    ifelse(delim == "\\t", "TAB", paste0("'", delim, "'")), "\n", sep = ""); flush.console()
cat("[", Sys.time(), "] 04c: primeras columnas = ",
    paste(head(header_names, 12), collapse=" | "), "\n", sep = ""); flush.console()

# ====== COLUMNAS QUE QUEREMOS (solo si existen) ======
flag_comorb <- c("DIABETES","EPOC","ASMA","INMUSUPR","HIPERTENSION",
                 "CARDIOVASCULAR","OBESIDAD","RENAL_CRONICA","TABAQUISMO")
flag_sever  <- c("NEUMONIA","INTUBADO","UCI")
base_cols   <- c("EDAD","FECHA_SINTOMAS","FECHA_INGRESO","FECHA_DEF")
wanted      <- c(base_cols, flag_comorb, flag_sever)

present <- intersect(wanted, header_names)
missing <- setdiff(wanted, header_names)
if (length(present) == 0) stop("No se encontraron columnas esperadas. Revisa el encabezado/archivo.")
if (length(missing) > 0) {
  cat("[", Sys.time(), "] 04d: columnas NO encontradas: ", paste(missing, collapse=", "), "\n", sep = ""); flush.console()
} else {
  cat("[", Sys.time(), "] 04d: todas las columnas esperadas están presentes.\n", sep = ""); flush.console()
}

# Spec de tipos SOLO para las presentes
cols_spec <- setNames(rep("string", length(present)), present)
if ("EDAD" %in% present) cols_spec["EDAD"] <- "double"

# Lectura robusta en Spark
cat("[", Sys.time(), "] 05: spark_read_csv() con spec y delimitador detectado\n", sep = ""); flush.console()
spark_delim <- if (delim == "\\t") "\t" else delim
df_raw <- spark_read_csv(
  sc, name = "df_raw", path = INPUT_CSV,
  header = TRUE, infer_schema = FALSE, memory = FALSE,
  delimiter = spark_delim, columns = cols_spec, quote = "\"", escape = "\""
)

# Acción mínima para confirmar
peek <- df_raw %>% head(5) %>% collect()
cat("[", Sys.time(), "] 05b: peek OK (5 filas)\n", sep = ""); flush.console()

# (Opcional) cache bronze Parquet — omitido en Windows si aparece NativeIO
tryCatch(
  {
    spark_write_parquet(df_raw, path = file.path(OUT_DIR, "bronze_csv_cache"), mode = "overwrite")
  },
  error = function(e) {
    cat("[", Sys.time(), "] 05c: bronze Parquet OMITIDO por Windows NativeIO: ", conditionMessage(e), "\n", sep = "")
  }
)

# ====================== PREP. DE VARIABLES ====================
# Fechas: coalesce de formatos — usar SQL directo para parseo robusto
df <- df_raw %>%
  mutate(
    FECHA_SINTOMAS = !!sql("coalesce(to_date(FECHA_SINTOMAS, 'M/d/yyyy'), to_date(FECHA_SINTOMAS, 'yyyy-MM-dd'))"),
    FECHA_INGRESO  = !!sql("coalesce(to_date(FECHA_INGRESO , 'M/d/yyyy'), to_date(FECHA_INGRESO , 'yyyy-MM-dd'))"),
    FECHA_DEF      = !!sql("coalesce(to_date(FECHA_DEF     , 'yyyy-MM-dd'), to_date(FECHA_DEF     , 'M/d/yyyy'))")
  ) %>%
  # Normaliza EDAD (por si viene con caracteres)
  mutate(
    EDAD = as.double(!!sql("regexp_replace(COALESCE(CAST(EDAD AS STRING), ''), '[^0-9\\.-]', '')"))
  ) %>%
  # Fechas válidas: SINTOMAS <= INGRESO; edades plausibles 0..120
  filter( (is.na(EDAD) | (EDAD >= 0 & EDAD <= 120)) &
            (is.na(FECHA_SINTOMAS) | is.na(FECHA_INGRESO) | FECHA_SINTOMAS <= FECHA_INGRESO) ) %>%
  # Tiempos derivados
  mutate(
    t_ingreso_desde_sintomas = !!sql("datediff(FECHA_INGRESO, FECHA_SINTOMAS)"),
    t_def_desde_ingreso      = !!sql("datediff(FECHA_DEF, FECHA_INGRESO)"),
    t_def_desde_sintomas     = !!sql("datediff(FECHA_DEF, FECHA_SINTOMAS)")
  )

cat("[", Sys.time(), "] 06: fechas y tiempos derivados listos\n", sep = ""); flush.console()

# ===== Binarización robusta de flags y scores =====
flags <- intersect(c(flag_comorb, flag_sever), colnames(df))
if (length(flags) > 0) {
  df <- df %>%
    mutate(
      across(
        all_of(flags),
        ~ ifelse(
            (. == 1) | (toupper(cast(., "string")) == "SI") | (cast(., "string") == "1"),
            1.0, ifelse(
              (. == 2) | (toupper(cast(., "string")) == "NO") | (cast(., "string") == "2"),
              0.0, NA_real_
            )
          )
      )
    ) %>%
    mutate(
      score_comorbilidades = rowSums(select(., any_of(flag_comorb)), na.rm = TRUE),
      score_gravedad       = rowSums(select(., any_of(flag_sever)),  na.rm = TRUE)
    )
}

cat("[", Sys.time(), "] 07: flags binarizados (sin NA) y scores listos\n", sep = ""); flush.console()

# ===== ID estable y columnas originales a conservar para el perfil =====
df <- sdf_with_sequential_id(df, id = "rid")

cols_orig_to_keep <- intersect(
  c("rid", "EDAD", "t_ingreso_desde_sintomas", "t_def_desde_ingreso",
    "t_def_desde_sintomas", "score_comorbilidades", "score_gravedad"),
  colnames(df)
)
df_work <- df %>% select(all_of(cols_orig_to_keep))

# ================== SELECTOR A1 / A2 ==========================
if (MODO == "A2") {
  # Solo fallecidos: FECHA_DEF no nula
  df_work <- df_work %>% filter(!is.na(!!sql("rid"))) %>%
    inner_join(df %>% select(rid, FECHA_DEF), by = "rid") %>%
    filter(!is.na(FECHA_DEF)) %>% select(-FECHA_DEF)
  cat("[", Sys.time(), "] >> MODO A2: solo fallecidos\n", sep = ""); flush.console()
} else {
  cat("[", Sys.time(), "] >> MODO A1: perfiles generales\n", sep = ""); flush.console()
}

# Conjunto de variables numéricas candidatas (si existen)
num_cols <- intersect(
  c("EDAD", "t_ingreso_desde_sintomas", "score_comorbilidades", "score_gravedad"),
  colnames(df_work)
)
if (length(num_cols) < 2) stop("Muy pocas columnas numéricas disponibles para clustering.")

df_num <- df_work %>% select(all_of(num_cols))

# Limpieza de extremos en tiempos (si existen)
if ("t_ingreso_desde_sintomas" %in% num_cols) {
  df_num <- df_num %>%
    mutate(t_ingreso_desde_sintomas =
             ifelse( (cast(t_ingreso_desde_sintomas, "bigint") < -30) |
                     (cast(t_ingreso_desde_sintomas, "bigint") > 400), NA, t_ingreso_desde_sintomas))
}

if ("t_def_desde_ingreso" %in% num_cols) {
  df_num <- df_num %>%
    mutate(t_def_desde_ingreso =
             ifelse( (cast(t_def_desde_ingreso, "bigint") < -30) |
                     (cast(t_def_desde_ingreso, "bigint") > 400), NA, t_def_desde_ingreso))
}

if ("t_def_desde_sintomas" %in% num_cols) {
  df_num <- df_num %>%
    mutate(t_def_desde_sintomas =
             ifelse( (cast(t_def_desde_sintomas, "bigint") < -30) |
                     (cast(t_def_desde_sintomas, "bigint") > 400), NA, t_def_desde_sintomas))
}

cat("[", Sys.time(), "] 08: dataset numérico listo: ", paste(num_cols, collapse = ", "), "\n", sep = ""); flush.console()

# --- Guard-rails ANTES de imputar ---
# Conservar filas con al menos 1 valor no nulo en num_cols
df_num <- df_num %>% filter(if_any(all_of(num_cols), ~ !is.na(.)))
# Asegurar tipo double
df_num <- df_num %>% mutate(across(all_of(num_cols), as.double))
# Contar no-NA por columna con 0/1 (evita boolean en SUM)
nn <- df_num %>%
  summarise(across(all_of(num_cols), ~ sum(IF(is.na(.), 0L, 1L)))) %>%
  collect()
keep_cols <- names(nn)[as.numeric(nn[1, ]) > 0]
drop_cols <- setdiff(num_cols, keep_cols)
if (length(drop_cols) > 0) {
  cat("[", Sys.time(), "] 08b: columnas con todos NA removidas: ", paste(drop_cols, collapse = ", "), "\n", sep = ""); flush.console()
}
if (length(keep_cols) < 2) stop("Tras limpieza, quedan menos de 2 columnas con datos: ", paste(keep_cols, collapse=", "))
num_cols <- keep_cols
df_num   <- df_num %>% select(all_of(num_cols))

# Diagnóstico rápido
diag <- df_num %>% summarise(
  n = n(), across(all_of(num_cols), list(nn = ~ sum(IF(is.na(.), 0L, 1L)), mean = ~ mean(., na.rm = TRUE)))
) %>% collect()
print(diag)

# ================== IMPUTACIÓN + ESCALADO =====================
# Imputación (median)
imp_est   <- ft_imputer(sc, input_cols = num_cols, output_cols = paste0(num_cols, "_imp"), strategy = "median")
imp_model <- ml_fit(imp_est, df_num)
df_imp    <- ml_transform(imp_model, df_num)

# Ensamblado + estandarizado
assembler <- ft_vector_assembler(sc, input_cols = paste0(num_cols, "_imp"), output_col = "features_raw")
scaler    <- ft_standard_scaler(sc, input_col = "features_raw", output_col = "features", with_mean = TRUE, with_std = TRUE)
prep_model <- ml_fit(ml_pipeline(assembler, scaler), df_imp)
df_prep    <- ml_transform(prep_model, df_imp)
cat("[", Sys.time(), "] 09: imputación y escalado listos\n", sep = ""); flush.console()

# ======================== PCA / PROYECCIÓN 2D ===============================
feat_count <- length(num_cols)
pca_out <- "pca_features"

if (feat_count >= 3) {
  # PCA con 3+ variables
  pca_model <- ml_pca(
    x          = df_prep,
    input_col  = "features",
    output_col = pca_out,
    k          = 2
  )
  df_pca <- ml_transform(pca_model, df_prep)
  # Separa el vector de 2 componentes en pc1/pc2 (sin usar vector_to_array)
  df_pca <- sparklyr.nested::sdf_separate_column(df_pca, pca_out, into = c("pc1", "pc2")) %>%
    select(pc1, pc2, everything())
} else {
  # Sin PCA (solo 2 features): separa directamente el vector 'features'
  df_pca <- sparklyr.nested::sdf_separate_column(df_prep, "features", into = c("pc1","pc2")) %>%
    select(pc1, pc2, everything())
}
cat("[", Sys.time(), "] 10: proyección 2D lista (", ifelse(feat_count >= 3, "PCA", "sin PCA"), ")\n", sep = ""); flush.console()

# ================== SELECCIÓN DE K (Silhouette) ======================
cat("[", Sys.time(), "] 10b: preparando muestra para selección de k (silhouette)...\n", sep = ""); flush.console()

# Muestra para acelerar (ajusta fraction según tu RAM/tiempo)
df_prep_k <- df_prep %>%
  sdf_sample(fraction = 0.08, replacement = FALSE, seed = 777L) %>%
  sdf_register("df_prep_k_cache")
tbl_cache(sc, "df_prep_k_cache")
invisible(count(df_prep_k))

ks   <- 2:10
sils <- rep(NA_real_, length(ks))

cat("[", Sys.time(), "] 10c: loop k=2..10 midiendo silhouette\n", sep = ""); flush.console()
for (i in seq_along(ks)) {
  k_val <- ks[i]
  cat("[", Sys.time(), "]   - Entrenando k=", k_val, " ... ", sep = ""); flush.console()
  # Entrena en la muestra cacheada
  m_k <- try(
    ml_kmeans(df_prep_k, k = k_val, features_col = "features", seed = 123L, max_iter = 60L),
    silent = TRUE
  )
  if (inherits(m_k, "try-error")) { cat("ERROR\n"); next }
  pred_k <- ml_transform(m_k, df_prep_k)
  # Evaluador silhouette
  ev <- ml_clustering_evaluator(sc, prediction_col = "prediction", features_col = "features", metric_name = "silhouette")
  s  <- try(ml_evaluate(ev, pred_k), silent = TRUE)
  if (inherits(s, "try-error") || is.null(s) || length(s) == 0) {
    cat("OK, silhouette=NA\n")
    sils[i] <- NA_real_
  } else {
    sils[i] <- as.numeric(s)
    cat(sprintf("OK, silhouette=%.5f\n", sils[i]))
  }
}
tbl_uncache(sc, "df_prep_k_cache")

if (all(is.na(sils))) stop("No se pudo calcular silhouette para ningún k (revisa ml_clustering_evaluator).")

# Guarda gráfico de silhouette
sil_df  <- data.frame(k = ks, silhouette = sils)
p_sil   <- ggplot(sil_df, aes(k, silhouette)) + geom_line() + geom_point() +
  theme_minimal() + labs(title = paste0("Silhouette vs k — ", MODO), x = "k", y = "Silhouette")
ggsave(filename = .path_out(paste0("silhouette_k_", MODO, ".png")),
       plot = p_sil, width = 10, height = 6, dpi = 120)
cat("[", Sys.time(), "] 10d: gráfico de silhouette guardado en ", .path_out(paste0("silhouette_k_", MODO, ".png")), "\n", sep = ""); flush.console()

# Elegir k (máximo silhouette; si empata, el menor k)
valid  <- which(!is.na(sils))
best_k <- ks[ valid[ which.max(sils[valid]) ] ]
cat("[", Sys.time(), "] 10e: k elegido (silhouette) = ", best_k, "\n", sep = ""); flush.console()

# Modelo final con TODO el df_prep
km   <- ml_kmeans(df_prep, k = best_k, features_col = "features", seed = 42L, max_iter = 100L)

# Predicción sobre la proyección 2D (df_pca contiene pc1/pc2 + features/imp)
pred <- ml_transform(km, df_pca)  # añade 'prediction' manteniendo pc1/pc2
cat("[", Sys.time(), "] 11: K-means final entrenado y aplicado\n", sep = ""); flush.console()

# ================== PERFILES DE CLÚSTER =======================
# Verifica que 'rid' esté presente (viaja desde df -> df_work -> df_imp/df_prep/df_pca)
if (!("rid" %in% colnames(pred))) {
  stop("No se encontró 'rid' en 'pred'. Verifica que agregaste 'rid' en df y que lo preservaste hasta df_prep/df_pca.")
}

# Une predicciones con variables originales para perfilar
pred_join <- pred %>%
  select(rid, prediction) %>%
  left_join(df_work %>% select(all_of(cols_orig_to_keep)), by = "rid")

perfil <- pred_join %>%
  group_by(prediction) %>%
  summarise(
    n               = n(),
    edad_mean       = if ("EDAD"                     %in% colnames(.)) mean(as.double(EDAD), na.rm = TRUE) else NA_real_,
    t_ingreso_mean  = if ("t_ingreso_desde_sintomas" %in% colnames(.)) mean(as.double(t_ingreso_desde_sintomas), na.rm = TRUE) else NA_real_,
    t_def_ing_mean  = if ("t_def_desde_ingreso"      %in% colnames(.)) mean(as.double(t_def_desde_ingreso), na.rm = TRUE) else NA_real_,
    t_def_sint_mean = if ("t_def_desde_sintomas"     %in% colnames(.)) mean(as.double(t_def_desde_sintomas), na.rm = TRUE) else NA_real_,
    comorb_mean     = if ("score_comorbilidades"     %in% colnames(.)) mean(as.double(score_comorbilidades), na.rm = TRUE) else NA_real_,
    gravedad_mean   = if ("score_gravedad"           %in% colnames(.)) mean(as.double(score_gravedad), na.rm = TRUE) else NA_real_
  ) %>%
  arrange(desc(n)) %>%
  collect()

write.csv(perfil, file = .path_out(paste0("perfil_clusters_numeric_", MODO, ".csv")), row.names = FALSE)
cat("[", Sys.time(), "] 12: perfil de clústeres guardado\n", sep = ""); flush.console()

# ================== GRÁFICA PCA (muestra) =====================
plot_df <- pred %>%
  sdf_sample(fraction = 0.2, replacement = FALSE, seed = 7L) %>%
  select(pc1, pc2, prediction) %>%
  collect()

p <- ggplot(plot_df, aes(pc1, pc2, color = factor(prediction))) +
  geom_point(alpha = 0.6, size = 1.0) +
  labs(color = "Cluster",
       title = paste0("K-means (k = ", km$k, ") — Proyección 2D — ", MODO),
       x = "PC1 / F1", y = "PC2 / F2") +
  theme_minimal()

ggsave(filename = .path_out(paste0("pca_clusters_numeric_", MODO, ".png")),
       plot = p, width = 10, height = 6, dpi = 120)
cat("[", Sys.time(), "] 13: gráfico PCA guardado en ", .path_out(paste0("pca_clusters_numeric_", MODO, ".png")), "\n", sep = ""); flush.console()

cat("[", Sys.time(), "] 14: FIN OK\n", sep = ""); flush.console()
