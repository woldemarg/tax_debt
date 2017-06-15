library(readr)

#ФОП из ЕГРПОУ
import_fop <- function(file) {
  db <-
    read_csv(
      file,
      skip = 1,
      trim_ws = TRUE,
      locale = locale(encoding = "UTF-8"),
      col_names = c("name",
                    "addr",
                    "kved",
                    "stat")
    )
  return(db)
}
