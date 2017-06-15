library(tidyverse)
library(stringr)


# Upload raw data ----

source("funcs/get_raw_fop.R")
fop <- import_fop("data_main/fop.csv.gz")

source("funcs/get_pst_ind.R")
pi <- getPostInds("data_sppl/postind.csv.gz") %>%
  within(pst_ind <-
           as.character(str_extract(pst_ind, "[0-9]{2}"))) %>%
  unique()


# Get index from address ----

fop$index <-
  as.character(str_sub(str_extract(fop$addr, "[0-9]{5}"), 1, 2))

fop$index[is.na(fop$index)] <-
  ifelse((grepl("^[^0]", fop$addr[is.na(fop$index)]) &
            grepl("^[0-9]{2,}", fop$addr[is.na(fop$index)])),  as.character(str_sub(
              str_replace(fop$addr[is.na(fop$index)], "^([0-9]{2,})\\,.*", "\\1"), 1, 2
            )),
         NA)


# Match region by index ----

fop <- fop %>% within(reg <- pi$reg_ua[match(index, pi$pst_ind)])


# Get region from string ----

fop$reg[is.na(fop$reg)] <-
  ifelse(
    grepl("^[^ ]+\\sобл\\.", fop$addr[is.na(fop$reg)]),
    str_replace(fop$addr[is.na(fop$reg)], "\\s?(.*)\\sобл\\.\\,.*", "\\1"),
    NA
  )

fop$reg[is.na(fop$reg)] <-
  str_extract(fop$addr[is.na(fop$reg)], paste(regs, collapse = "|"))

fop$reg[is.na(fop$reg)] <-
  str_to_title(str_extract(fop$addr[is.na(fop$reg)], paste(str_to_upper(regs), collapse = "|")))


# Hardcode region ----

fop$reg[is.na(fop$reg)] <-
  ifelse(
    grepl("АР КРИМ|ЯЛТА|ФЕОДОСіЯ|ЄВПАТОРІЯ", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
    "Автономна Республіка Крим",
    ifelse(
      grepl("ЧЕРКАСИ", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
      "Черкаська",
      ifelse(
        grepl("ЛУГАНСЬК", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
        "Луганська",
        ifelse(
          grepl("ДНІПРОПЕТРОВСЬК", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
          "Дніпропетровська",
          ifelse(
            grepl("ОДЕСА", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
            "Одеська",
            ifelse(
              grepl("ХАРКІВ", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
              "Харківська",
              ifelse(
                grepl("КІРОВОГРАД", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
                "Кіровоградська",
                ifelse(
                  grepl("Миколаїв", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
                  "Миколаївська",
                  ifelse(
                    grepl("Бровари|Глеваха", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
                    "Київська",
                    ifelse(
                      grepl("ЛЬВІІВСЬКА", fop$addr[is.na(fop$reg)], ignore.case = TRUE),
                      "Львівська",
                      NA
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  )


# Save data ----

write_csv(fop, gzfile("data_calc/fop_regs.csv.gz"))
