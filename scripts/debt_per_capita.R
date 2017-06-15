library(tidyverse)
library(forcats)
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

#население по регионам на 01.04.2017
pop <- read_csv("data_sppl/population.csv")


# Adding regions ----

db_fop$reg <-
  tax_deps$reg[match(db_fop$tx_dep, tax_deps$tax)]


# Converting to factors ----

cols <- c("reg", "status", "ctg")
db_uo[cols] <- lapply(db_uo[cols], factor)
db_uo$status <- fct_explicit_na(db_uo$status)
db_uo %<>%
  mutate(status = fct_collapse(
    status,
    live = c("зареєстровано"),
    dead = c(
      "в стані припинення",
      "зареєстровано, свідоцтво про державну реєстрацію недійсне",
      "порушено справу про банкрутство",
      "порушено справу про банкрутство (санація)",
      "припинено",
      "(Missing)"
    )
  ))


# Grouping by region ----

group_uo <- db_uo %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь") %>%
  group_by(reg, status) %>%
  summarise(loc = sum(dt_loc), gov = sum(dt_gov))

#после исключения ФОПа из ЕГРПОУ его обязательства
#превращаются в обязательства физлица, поэтому
#все должники-ФОПы получают статус "действующих"
#http://zakon2.rada.gov.ua/laws/show/2343-12
group_fop <- db_fop %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь") %>%
  group_by(reg) %>%
  summarise(loc = sum(dt_loc), gov = sum(dt_gov)) %>%
  mutate(status = "live")

group_all <- union(group_uo, group_fop)

#http://www.cookbook-r.com/Manipulating_data/Summarizing_data/
db_all <-
  aggregate(group_all[c("loc", "gov")],
            by = group_all[c("reg", "status")],
            FUN = sum,
            na.rm = TRUE)


# Debt per capita ----

#х1000 для перехода от тыс. грн. к грн.
debt_gov_live_per_capita <-
  sum(db_all$gov[db_all$status != "live"]) * 1000 / sum(pop$pop[pop$reg != "Автономна Республіка Крим" &
                                                                  pop$reg != "Севастополь"])
debt_gov_dead_per_capita <-
  sum(db_all$gov[db_all$status != "dead"]) * 1000 / sum(pop$pop[pop$reg != "Автономна Республіка Крим" &
                                                                  pop$reg != "Севастополь"])

db_all$db_gov_per_capita <-
  ifelse(db_all$status == "live",
         debt_gov_live_per_capita,
         debt_gov_dead_per_capita)

db_all %<>% within(db_loc_per_capita <-
                     .$loc * 1000 / pop$pop[match(.$reg, pop$reg)]) %>%
  mutate(db_sum = db_gov_per_capita + db_loc_per_capita)


# Saving output ----

db_per_capita <- db_all %>% select(-3:-6)
write_csv(db_per_capita, gzfile("data_calc/debt_per_capita.csv.gz"))
