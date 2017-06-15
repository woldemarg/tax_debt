library(dplyr)
library(stringr)

#юрлица из ЕДРПОУ
source("funcs/get_raw_uo.R")
uo <- import_uo("data_main/UO.csv.gz")

#выделяем стандартный почтовый индекс из адреса (5 знаков)
#оставляем первые два для идентификации области
uo$index <-
  as.character(str_sub(str_extract(uo$addr, "[0-9]{5}"), 1, 2))

#обработка неполных индексов в начале адреса
#если не начинается на ноль и имеет больше двух знаков
#оставляем первые два для идентификации области
uo$index[is.na(uo$index)] <-
  ifelse((grepl("^[^0]", uo$addr[is.na(uo$index)]) &
            grepl("^[0-9]{2,}", uo$addr[is.na(uo$index)])),  as.character(str_sub(
              str_replace(uo$addr[is.na(uo$index)], "^([0-9]{2,})\\,.*", "\\1"), 1, 2
            )),
         NA)

#список почтовых кодов
#первые два знака для области
#03027 - Киев, а не Киевская обл., с. Новоселки (изм. вручную)
source("funcs/get_pst_ind.R")
pi <- getPostInds("data_sppl/postind.csv.gz") %>%
  within(pst_ind <-
           as.character(str_extract(pst_ind, "[0-9]{2}"))) %>%
  unique()

#определение областей по почтовому индексу
uo <- uo %>% within(reg <- pi$reg_ua[match(index, pi$pst_ind)])

#выбор названия области из строки в начале адреса
uo$reg[is.na(uo$reg)] <-
  ifelse(
    grepl("^[^ ]+\\sобл\\.", uo$addr[is.na(uo$reg)]),
    str_replace(uo$addr[is.na(uo$reg)], "\\s?(.*)\\sобл\\.\\,.*", "\\1"),
    NA
  )

#некоторые строки имеб двойное указание области
#в верхнем и нижнем регистре
#делаем выборку за два прохода
uo$reg[is.na(uo$reg)] <-
  ifelse(
    grepl("^[^ ]+\\sОБЛ\\.", uo$addr[is.na(uo$reg)]),
    str_replace(uo$addr[is.na(uo$reg)], "\\s?(.*)\\sОБЛ\\.\\,.*", "\\1"),
    NA
  )

#таблица несоответствий
nm <- uo %>% filter(is.na(reg))

#hard code
#pучная замена пробелов в таблице несоответствий
nm$reg <-
  ifelse((is.na(nm$reg) & grepl("обл\\.", nm$addr)),
         str_replace(nm$addr, ".*\\s(.*)\\sобл\\..*", "\\1"),
         ifelse((
           is.na(nm$reg) &
             grepl("(київ\\s?\\,|києво|КИЇІВ)", nm$addr, ignore.case = TRUE)
         ),
         "Київ",
         ifelse((is.na(nm$reg) &
                   grepl("КІРОВОГРАД", nm$addr, ignore.case = TRUE)),
                "Кіровоградська",
                ifelse((is.na(nm$reg) &
                          grepl("ЖИТОМИР", nm$addr, ignore.case = TRUE)),
                       "Житомирська",
                       ifelse((is.na(nm$reg) &
                                 grepl("МИКОЛАЇВ", nm$addr, ignore.case = TRUE)),
                              "Миколаївська",
                              ifelse((
                                is.na(nm$reg) & grepl("ЛУГАНСЬК", nm$addr, ignore.case = TRUE)
                              ),
                              "Луганська",
                              ifelse((is.na(nm$reg) &
                                        grepl("ХАРКІВ", nm$addr, ignore.case = TRUE)),
                                     "Харківська",
                                     ifelse((is.na(nm$reg) &
                                               grepl("ЛЬВІВ", nm$addr, ignore.case = TRUE)),
                                            "Львівська",
                                            ifelse((
                                              is.na(nm$reg) & grepl("СЕВАСТОПОЛЬ", nm$addr, ignore.case = TRUE)
                                            ),
                                            "Севастополь",
                                            ifelse((is.na(nm$reg) &
                                                      grepl("КРИМ", nm$addr, ignore.case = TRUE)),
                                                   "Автономна Республіка Крим",
                                                   ifelse((
                                                     is.na(nm$reg) & grepl("ВІННИЦЯ", nm$addr, ignore.case = TRUE)
                                                   ),
                                                   "Вінницька",
                                                   ifelse((is.na(nm$reg) &
                                                             grepl("ЗАПОРІЖЖЯ", nm$addr, ignore.case = TRUE)),
                                                          "Запорізька",
                                                          ifelse((
                                                            is.na(nm$reg) &
                                                              grepl("(КРИВИЙ РІГ|ДНІПРОПЕТРОВСЬК)", nm$addr, ignore.case = TRUE)
                                                          ),
                                                          "Дніпропетровська",
                                                          ifelse((
                                                            is.na(nm$reg) & grepl("СУМИ", nm$addr, ignore.case = TRUE)
                                                          ),
                                                          "Сумська",
                                                          ifelse((
                                                            is.na(nm$reg) & grepl("ГОГОЛІВ", nm$addr, ignore.case = TRUE)
                                                          ),
                                                          "Київська",
                                                          NA))
                                                          )
                                                   ))
                                            )
                                            )
                                     )
                              ))
                       )
                )
         ))
  )

#перенос данных из заполненной таблицы в основную БД
uo <-
  within(uo, reg[is.na(reg)] <-
           nm$reg[match(edrpou[is.na(reg)], nm$edrpou)])

#проверка на отсутствие незаполненных полей в основной БД
#и названий областей
sum(is.na(uo$reg)) == 0
sort(unique(uo$reg))

#сохраняем файл
write_csv(uo, gzfile("data_calc/uo_regs.csv.gz"))
