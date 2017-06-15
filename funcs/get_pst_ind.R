library(readr)
library(dplyr)

#почтовые индексы
getPostInds <- function(file) {
  db <-
    read_csv2(
      file,
      skip = 3,
      #metadata
      trim_ws = TRUE,
      locale = locale(encoding = "UTF-8"),
      col_names = c(
        "reg_ua",
        "dist_ua",
        "city_ua",
        "strn_ua",
        "nums_ua",
        "pst_ua",
        "pst_ind",
        "reg_en",
        "dist_en",
        "city_en",
        "pst_en",
        "strn_en",
        "nums_en",
        "ind_b3"
      ),
      col_types = cols(.default = col_character()) #индекс как текст
    ) %>%
    select(-2:-6, -8:-14)
  return(unique(db)) #remove dupls
}
