library(tidyverse)
library(magrittr)


# Loading data ----

#должники-ФОПы
source("funcs/get_dbtrs.R")
db_fop <- import_dbtrs("data_main/dbtrs.csv.gz") %>%
  filter(is.na(dhead)) %>%
  select(1, 3:5)

#должники-юрлица
db_uo <- read_csv("data_calc/dbt_idntfd.csv.gz")

#адреса налоговых инспекций
tax_deps <- read_csv("data_sppl/tax_deps.csv.gz", skip = 2)


# Adding regions ----

db_fop$reg <-
  tax_deps$reg[match(db_fop$tx_dep, tax_deps$tax)]


# Grouping by region ----

group_uo <- db_uo %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь") %>%
  group_by(reg) %>%
  summarise(loc = sum(dt_loc * 1000), gov = sum(dt_gov * 1000))

group_fop <- db_fop %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь") %>%
  group_by(reg) %>%
  summarise(loc = sum(dt_loc * 1000), gov = sum(dt_gov * 1000))


group_all <- union(group_uo, group_fop)

#http://www.cookbook-r.com/Manipulating_data/Summarizing_data/
db_total <-
  aggregate(group_all[c("loc", "gov")],
            by = group_all["reg"],
            FUN = sum,
            na.rm = TRUE)

# Saving output ----

write_csv(db_total, gzfile("data_calc/debt_total.csv.gz"))
