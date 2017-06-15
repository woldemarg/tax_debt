library(readr)
library(dplyr)

#должники
import_dbtrs <- function(file) {
  db <-
    read_csv2(
      file,
      skip = 5,
      trim_ws = TRUE,
      locale = locale(encoding = "UTF-8"),
      col_names = c(
        ".",
        "dname",
        "dhead",
        "tx_dep",
        ".",
        "dt_gov",
        "dt_loc",
        "."
      )
    ) %>%
    select(-1, -5, -8)
  return(db)
}
