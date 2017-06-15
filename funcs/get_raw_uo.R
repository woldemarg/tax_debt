library(readr)

#юрлица из ЕГРПОУ
import_uo <- function(file) {
  db <-
    read_csv(
      file,
      skip = 3,
      trim_ws = TRUE,
      locale = locale(encoding = "UTF-8"),
      col_names = c("nm_fl",
                    "nm_sh",
                    "edrpou",
                    "addr",
                    "hd_nm",
                    "kved",
                    "stat")
    )
  return(db)
}
