# Função
baixar_chirps <- function(
    start_date, end_date, kml_path,
    out_root = "/home/labhidro/Bruno/PRECIPITATION/HISTORICAL",
    workers = 6,
    batch_size = 200,         # quantos dias por lote
    retries = 3,              # tentativas por arquivo
    min_size = 20000,         # bytes mínimos p/ considerar OK (~20 KB)
    timeout_sec = 120,        # timeout por requisição
    pause_min = 1,            # backoff inicial (s)
    pause_cap = 10            # backoff máximo (s)
) {
  # deps
  library(httr)
  library(terra)
  library(sf)
  library(future.apply)
  
  # Base CHIRPS v3.0 daily/final/rnl
  base_url <- "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/"
  
  # Pasta de saída por período
  out_dir <- file.path(out_root, sprintf("CHIRPS_%s_%s", start_date, end_date))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  # Área de interesse
  aoi <- st_read(kml_path, quiet = TRUE)
  
  # Sequência de datas
  datas <- seq.Date(as.Date(start_date), as.Date(end_date), by = "day")
  
  # Arquivos esperados e já existentes
  files_expected <- sprintf("chirps-v3.0.rnl.%s.%s.%s.tif",
                            format(datas, "%Y"),
                            format(datas, "%m"),
                            format(datas, "%d"))
  files_existing <- list.files(out_dir, pattern = "\\.tif$", full.names = FALSE)
  
  # Filtrar apenas os dias faltantes (não baixados ainda)
  faltantes <- datas[!(files_expected %in% files_existing)]
  if (length(faltantes) == 0) {
    message("✅ Todos os arquivos deste intervalo já estão presentes em: ", out_dir)
    return(invisible(list(ok = character(0), falhas = character(0))))
  }
  
  message("🔎 Dias faltantes: ", length(faltantes),
          " (", start_date, " → ", end_date, ")")
  message("📂 Saída: ", out_dir)
  
  # Plano paralelo controlado
  plan(multisession, workers = workers)
  
  # Particionar em lotes para não sobrecarregar servidor / rede
  part_idx <- ceiling(seq_along(faltantes) / batch_size)
  lotes <- split(faltantes, part_idx)
  
  # Helper: HEAD para checar existência (404 vs 200)
  head_ok <- function(url) {
    resp <- try(httr::HEAD(url, httr::timeout(timeout_sec),
                           httr::user_agent("R-CHIRPS/1.0")), silent = TRUE)
    if (inherits(resp, "try-error")) return(NA_integer_)
    httr::status_code(resp)
  }
  
  # Helper: baixa 1 dia com RETRY + checagens
  processar_dia <- function(d) {
    ano <- format(d, "%Y"); mes <- format(d, "%m"); dia <- format(d, "%d")
    file_name <- sprintf("chirps-v3.0.rnl.%s.%s.%s.tif", ano, mes, dia)
    url_file  <- paste0(base_url, ano, "/", file_name)
    out_file  <- file.path(out_dir, file_name)
    
    # já existe (proteção dupla)
    if (file.exists(out_file) && file.size(out_file) >= min_size) {
      return(sprintf("⏩ Já existe: %s", file_name))
    }
    
    # 1) HEAD – se 404, não perde tempo
    sc <- head_ok(url_file)
    if (!is.na(sc) && sc == 404) {
      return(sprintf("🚫 404 (indisponível): %s", file_name))
    }
    
    # 2) RETRY: tenta baixar com backoff exponencial
    tmp <- tempfile(fileext = ".tif")
    ok <- FALSE; last_err <- NULL
    for (k in seq_len(retries)) {
      resp <- try(
        httr::RETRY("GET",
                    url_file,
                    httr::write_disk(tmp, overwrite = TRUE),
                    httr::timeout(timeout_sec),
                    httr::user_agent("R-CHIRPS/1.0"),
                    pause_min = pause_min, pause_cap = pause_cap, times = 1
        ),
        silent = TRUE
      )
      if (!inherits(resp, "try-error") && inherits(resp, "response") &&
          httr::status_code(resp) == 200 && file.exists(tmp) && file.size(tmp) >= min_size) {
        ok <- TRUE
        break
      } else {
        last_err <- if (inherits(resp, "try-error")) as.character(resp) else paste("HTTP", httr::status_code(resp))
      }
    }
    if (!ok) {
      return(sprintf("❌ Falha no download (%s): %s", ifelse(is.null(last_err), "erro", last_err), file_name))
    }
    
    # 3) Validar raster e recortar/mascarar
    r <- try(terra::rast(tmp), silent = TRUE)
    if (inherits(r, "try-error")) {
      return(sprintf("⚠️ Arquivo corrompido (rast erro): %s", file_name))
    }
    
    # recorte + máscara
    r_crop <- try(terra::mask(terra::crop(r, aoi), aoi), silent = TRUE)
    if (inherits(r_crop, "try-error")) {
      return(sprintf("⚠️ Falha no crop/mask: %s", file_name))
    }
    
    terra::writeRaster(r_crop, out_file, overwrite = TRUE)
    sprintf("✅ Baixado e processado: %s", file_name)
  }
  
  # Loop em lotes com paralelismo e checkpoint de falhas
  todas_falhas <- character(0)
  for (i in seq_along(lotes)) {
    lote <- lotes[[i]]
    message(sprintf("🧩 Lote %d/%d — %d dia(s)", i, length(lotes), length(lote)))
    
    res <- future_lapply(lote, processar_dia, future.seed = TRUE)
    
    # salvar log do lote
    log_path <- file.path(out_dir, sprintf("log_lote_%03d.txt", i))
    writeLines(unlist(res), log_path)
    
    # coletar falhas para re-tentar manualmente depois, se quiser
    falhas <- grep("^❌|^⚠️|^🚫", unlist(res), value = TRUE)
    if (length(falhas)) {
      todas_falhas <- c(todas_falhas, falhas)
      fail_list_path <- file.path(out_dir, sprintf("faltantes_lote_%03d.txt", i))
      writeLines(falhas, fail_list_path)
      message(sprintf("   ⚠️ %d problema(s) no lote %d — detalhes: %s",
                      length(falhas), i, fail_list_path))
    }
    
    # pequena pausa entre lotes (educação com o host 😉)
    Sys.sleep(3)
  }
  
  message("✅ Concluído! Pasta: ", out_dir)
  invisible(list(ok = NULL, falhas = todas_falhas))
}


# Executar função
baixar_chirps(
  start_date = "1981-01-01",
  end_date   = "2025-09-30",
  kml_path   = "/home/labhidro/Bruno/PRECIPITATION/sa.kml",
  workers    = 120,         # 6–8 é um bom equilíbrio
  batch_size = 200,       # processa 200 dias por lote
  retries    = 3,         # até 3 tentativas por dia
  min_size   = 20000      # ~20 KB
)