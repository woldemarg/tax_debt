library(tidyverse)

tot <- read_csv("data_calc/debt_total.csv.gz") %>%
  mutate(sum = loc + gov) %>%
  select(-3)

dpc <- read_csv("data_calc/debt_per_capita.csv.gz") %>%
  spread(key = status, value = db_sum) %>%
  mutate(prc = dead + live) %>%
  select(-3)

ind <- read_csv("data_calc/debt_index.csv.gz")

data <- tot %>% left_join(dpc, by = "reg") %>%
  left_join(ind, by = "reg")

data <- data[c(1, 3, 2, 5, 4, 6)] %>%
  mutate(
    sum = sum / 1000000000,
    loc = loc / 1000000000,
    index = index * 100
  )

write_csv(data, "data_calc/data.csv")
