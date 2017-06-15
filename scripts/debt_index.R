library(tidyverse)
library(forcats)
library(magrittr)


# Loading data ----

#юрлица из ЕДРПОУ
uo <- read_csv("data_calc/uo_regs.csv.gz")
db <- read_csv("data_calc/dbt_idntfd.csv.gz")


# Converting to factors ----

uo$status <- as.factor(uo$status)
uo %<>%
  mutate(status = fct_collapse(
    status,
    live = c("зареєстровано"),
    dead = c(
      "в стані припинення",
      "зареєстровано, свідоцтво про державну реєстрацію недійсне",
      "порушено справу про банкрутство",
      "порушено справу про банкрутство (санація)",
      "припинено"
    )
  ))

db$status <- as.factor(db$status)
db$status <- fct_explicit_na(db$status)
db %<>%
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

group_uo <- uo %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь" & status == "live") %>%
  group_by(reg) %>%
  summarise(count = n())

group_db <- db %>%
  filter(reg != "Автономна Республіка Крим" &
           reg != "Севастополь" & status == "live" & (dt_loc + dt_gov) != 0) %>%
  group_by(reg) %>%
  summarise(count = n())


# Calculating index ----

#отношение количества действующих должников
#к общему количеству действующих юрлиц
ind <- group_uo %>% left_join(group_db, by = "reg") %>%
  mutate(index = count.y / count.x)

# Saving output ----

db_ind <- ind %>% select(-2, -3)
write_csv(db_ind, gzfile("data_calc/debt_index.csv.gz"))
