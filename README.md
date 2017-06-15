## Анализ налогового долга

#### Описание проблемы
ДФС предоставляет исходные данные по налоговой задолженности в виде [реестра должников](http://data.gov.ua/passport/ae5a5a6e-f13e-44bf-89a3-fe71d90ab0a0), в котором представлена информация о наименовании предприятия, налоговой инспекции и сумме задолженности.

Для проведения даже простого анализа этих данных необходимо идентифицировать должников (найти дополнительные сведения о них в [ЕГРПОУ](http://data.gov.ua/passport/73cfe78e-89ef-4f06-b3ab-eb5f16aea237)). Это даст возможность рассматривать долг в разрезе действующих/ликвидированных предприятий, увидеть распределение должников по видам деятельности, рассчитать индекс налоговой дисциплины и т.д.
>**Индекс налоговой дисциплины** - отношение количества действующих должников к общему количеству действующих предприятий в регионе.

#### Алгоритм идентификации
Идентификация должников-юрлиц происходила на основе сопоставления наименования и региона регистрации предприятия из реестра ДФС с записями в ЕГРПОУ. При сопоставлении двух реестров необходимо принять во внимание, что **предприятия могут иметь одинаковые наименования**. Для уменьшения риска ошибочной идентификации из списков сопоставления исключались предприятия с идентичными названиями внутри региона. Подробнее эта и другие особенности идентификации описаны в комментариях к папке [scripts](https://github.com/woldemarg/tax-debt/tree/master/scripts/), а также прямо в комментариях внутри файлов с кодом.

Общее представление о процессе идентификации юрлиц.

Не идентифицированными остались ок. 10% должников.

#### Описание папок в репозитории
* **data_main** - исходные данные для анализа
* **data_sppl** - дополнительные БД
* **funcs** - вспомогательные функции чтения данных
* **scripts** - основные аналитические скрипты
* **data_calc** - результаты обработки данных, идентификации и расчетов