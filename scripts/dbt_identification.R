library(dplyr)
library(readr)
library(stringr)
library(foreach)
library(doParallel)


# Intro ----

# Критерии для идентификации должников в ЕГРПОУ:
#   - полное наименование предприятия
#   - краткое наименование предприятия
#   - ФИО руководителя
#
# Ограничения и проблемы идентификации:
#   - в ЕГРПОУ есть предприятия с идентичными названиями
#   - в ЕГРПОУ под одним кодом значатся разные юрлица (дейст. и недейст)
#   - в ЕГРПОУ есть записи без кодов (117 шт.)
#   - названия в списке должников могут совпадать и
#       с полным и с кратким названием предприятия в ЕГРПОУ
#   - в списке должников есть Большие Плательщики Налогов,
#       которые привязаны не к региональной налоговой, а к офису БПН
#   - названия предприятий в обоих списках имеют проблемы со спецсимволами
#   - ФИО руководителей одного предприятия в разныч списках
#       могут не совпадать
#   - одно лицо может значиться руководителем нескольких
#       предприятий (дейст. и недейст.)
#   - в списке должников есть предприятия с идентичными названиями
#
#   Решение проблем идентификации:
#   - определить регион для каждой записи в БД должников
#       по адресу налоговой инспекции
#   - сравнивать записи одновременно по двум критериям
#       (основной критерий + регион) в три последовательных
#       прохода: полное, краткое название, ФИО руководителя
#   - перед каждым проходом исключать из ЕГРПОУ дубликаты
#       предприятий по текущему критерию в разрезе регионов
#   - перед каждым проходом также исключать из списка должников
#       дубликаты (по названию и ФИО руководителя)#
#
#   Как улучшить результатов -> применение алгоритмов fuzzy matching
#     для неидентифицированных предприятий (~10%) с использованием
#     библиотеки stringdist (не реализовано)


# Loading data ----

#юрлица из ЕГРПОУ
uor <- read_csv("data_calc/uo_regs.csv.gz") %>%
  filter(!is.na(edrpou)) %>%
  select(-8)

#должники
source("funcs/get_dbtrs.R")
dbt <- import_dbtrs("data_main/dbtrs.csv.gz") %>%
  filter(!is.na(dhead)) #убираем ФОПов

#большие плательщики налогов
vpp <- read_csv("data_sppl/vpp.csv.gz")

#адреса налоговых инспекций
tax_deps <- read_csv("data_sppl/tax_deps.csv.gz", skip = 2)


# Declaring funcs ----

modify_string <- function(str) {
  str_mod <- str %>%
    str_replace_all("`", "\"") %>% #обратная кавычка
    str_replace_all("\'+", "\"") %>% #две одинарные кавычки подряд
    str_replace_all("\"+", "\"") %>% #две двойные кавычки подряд
    str_replace_all("\\s", "") %>% #удаляем все пробелы
    str_replace_all("\\.", "") #удаляем точки
  return(str_mod)
}

#параллельная обработка списков по регионам

registerDoParallel(cores = detectCores())

find_dupls_by_reg <- function(data, crt_col, reg_col = 8) {
  regs <- unique(data[, reg_col])
  crts <- names(data)[c(reg_col, crt_col)]
  foreach(
    i = 1:nrow(regs),
    .packages = c("dplyr"), #!!!
    .inorder = FALSE,
    .combine = "bind_rows"
  ) %dopar% {
    data %>%
      filter_(paste(names(data)[reg_col], "==", "'", regs[[i, 1]], "'", sep = "")) %>%
      filter_(paste("!is.na(", names(data)[crt_col], ")", sep = "")) %>%
      group_by_(.dots = crts) %>%
      summarise(count = n()) %>%
      filter(count > 1)
  }
}


# Merging with BigTaxPayers DB ----

dbt <- dbt %>%
  left_join(vpp, by = c("dname" = "dname", "dhead" = "dhead"))


# Adding regions ----

dbt$reg[is.na(dbt$reg)] <-
  tax_deps$reg[match(dbt$tx_dep[is.na(dbt$reg)], tax_deps$tax)]


# Modifying strings ----

dbt$nm_mod <- modify_string(dbt$dname)
uor$nm_sh_mod <- modify_string(uor$nm_sh)
uor$nm_fl_mod <- modify_string(uor$nm_fl)


# Merging by name_full ----

#для оптимизации алгоритма разбиваем список должников на три части,
#последовательно обрабатываем каждую, затем объединяем

dbt_dtct <- dbt %>%
  filter(!is.na(edrpou))

dbt_drop <-
  semi_join(dbt[is.na(dbt$edrpou),],
            find_dupls_by_reg(dbt[is.na(dbt$edrpou),], crt_col = 14, reg_col = 13),
            by = c("nm_mod", "reg"))

dbt_keep <-
  anti_join(dbt[is.na(dbt$edrpou),], dbt_drop, by = c("nm_mod", "reg"))

dbt_keep <- uor %>%
  filter(!(edrpou %in% dbt_dtct$edrpou)) %>%
  anti_join(find_dupls_by_reg(uor, crt_col = 10), by = c("nm_fl_mod", "reg")) %>% #!!!
  right_join(dbt_keep, by = c("nm_fl_mod" = "nm_mod", "reg" = "reg")) %>% #!!!
  within(nm_fl.y <- ifelse(is.na(nm_fl.y), nm_fl.x, nm_fl.y)) %>%
  within(nm_sh.y <- ifelse(is.na(nm_sh.y), nm_sh.x, nm_sh.y)) %>%
  within(edrpou.y <-
           ifelse(is.na(edrpou.y), edrpou.x, edrpou.y)) %>%
  within(addr.y <- ifelse(is.na(addr.y), addr.x, addr.y)) %>%
  within(kved.y <- ifelse(is.na(kved.y), kved.x, kved.y)) %>%
  within(status.y <-
           ifelse(is.na(status.y), status.x, status.y)) %>%
  select(-1:-7, -9) %>% #!!!
  rename(
    nm_fl = nm_fl.y,
    nm_sh = nm_sh.y,
    edrpou = edrpou.y,
    addr = addr.y,
    hd_nm = hd_nm.y,
    kved = kved.y,
    status = status.y,
    nm_mod = nm_fl_mod #!!!
  )


# Merging by name_short ----

dbt_keep <- uor %>%
  filter(!(edrpou %in% dbt_dtct$edrpou) |
           !(edrpou %in% dbt_keep$edrpou)) %>%
  anti_join(find_dupls_by_reg(uor, crt_col = 9), by = c("nm_sh_mod", "reg")) %>% #!!!
  right_join(dbt_keep, by = c("nm_sh_mod" = "nm_mod", "reg" = "reg")) %>% #!!!
  within(nm_fl.y <- ifelse(is.na(nm_fl.y), nm_fl.x, nm_fl.y)) %>%
  within(nm_sh.y <- ifelse(is.na(nm_sh.y), nm_sh.x, nm_sh.y)) %>%
  within(edrpou.y <-
           ifelse(is.na(edrpou.y), edrpou.x, edrpou.y)) %>%
  within(addr.y <- ifelse(is.na(addr.y), addr.x, addr.y)) %>%
  within(kved.y <- ifelse(is.na(kved.y), kved.x, kved.y)) %>%
  within(status.y <-
           ifelse(is.na(status.y), status.x, status.y)) %>%
  select(-1:-7, -10) %>% #!!!
  rename(
    nm_fl = nm_fl.y,
    nm_sh = nm_sh.y,
    edrpou = edrpou.y,
    addr = addr.y,
    hd_nm = hd_nm.y,
    kved = kved.y,
    status = status.y,
    nm_mod = nm_sh_mod #!!!
  )

dbt <- bind_rows(dbt_keep, dbt_drop, dbt_dtct)

# Merging by head_name ----

#корректировка ФИО
uor$hd_nm <-
  str_replace_all(uor$hd_nm, "\\s+", " ") #несколько пробелов подряд
dbt$dhead <-
  str_replace_all(dbt$dhead, "\\s+", " ")

dbt_dtct <- dbt %>%
  filter(!is.na(edrpou))

dbt_drop <-
  semi_join(dbt[is.na(dbt$edrpou),],
            find_dupls_by_reg(dbt[is.na(dbt$edrpou),], crt_col = 4, reg_col = 1),
            by = c("dhead", "reg"))

dbt_keep <-
  anti_join(dbt[is.na(dbt$edrpou),], dbt_drop, by = c("dhead", "reg"))

dbt_keep <- uor %>%
  filter(!(edrpou %in% dbt_dtct$edrpou)) %>%
  anti_join(find_dupls_by_reg(uor, crt_col = 5), by = c("hd_nm", "reg")) %>% #!!!
  right_join(dbt_keep, by = c("hd_nm" = "dhead", "reg" = "reg")) %>% #!!!
  within(nm_fl.y <-
           ifelse(is.na(nm_fl.y), nm_fl.x, nm_fl.y)) %>%
  within(nm_sh.y <- ifelse(is.na(nm_sh.y), nm_sh.x, nm_sh.y)) %>%
  within(edrpou.y <-
           ifelse(is.na(edrpou.y), edrpou.x, edrpou.y)) %>%
  within(addr.y <- ifelse(is.na(addr.y), addr.x, addr.y)) %>%
  within(kved.y <- ifelse(is.na(kved.y), kved.x, kved.y)) %>%
  within(status.y <-
           ifelse(is.na(status.y), status.x, status.y)) %>%
  select(-1:-4, -6, -7, -9, -10) %>% #!!!
  rename(
    nm_fl = nm_fl.y,
    nm_sh = nm_sh.y,
    edrpou = edrpou.y,
    addr = addr.y,
    hd_nm = hd_nm.y,
    kved = kved.y,
    status = status.y,
    dhead = hd_nm #!!!
  )

dbt <- bind_rows(dbt_keep, dbt_drop, dbt_dtct)


# Error cutting ----

#предприятие Х в ЕГРПОУ имеет две формы названия: полную и короткую.
#Короткое название пр. Х может совпадать с названием пр. А из должников,
#а полное название пр. Х может совпадать с названием пр. Б.
#Разницу можно найти по ФИО руководителей

dpls <- dbt %>%
  filter(!is.na(edrpou)) %>%
  group_by(edrpou) %>%
  summarise(count = n()) %>%
  filter(count > 1) %>%
  left_join(uor, by = "edrpou") %>%
  select(1, 6)

cols <-
  c("nm_fl", "nm_sh", "edrpou", "addr", "hd_nm", "kved", "status")

for (i in 1:nrow(dpls)) {
  rows <- dbt[dbt$edrpou == dpls$edrpou[i] & !is.na(dbt$edrpou), ]
  for (j in 1:nrow(rows)) {
    if (rows$dhead[j] != dpls$hd_nm[i]) {
      dbt[dbt$edrpou == dpls$edrpou[i] &
            !is.na(dbt$edrpou) &
            dbt$dhead == rows$dhead[j], cols] <- NA
    }
  }
}

# Adding categories ----

all_dps <- read_csv("data_sppl/dp_register.csv.gz")
dbt$ctg <- ifelse(dbt$edrpou %in% all_dps$edrpou, "dp", "le")


# Saving output ----

dbt <- dbt %>% select(-3, -12)
write_csv(dbt, gzfile("data_calc/dbt_idntfd.csv.gz"))
