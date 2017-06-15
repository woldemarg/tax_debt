## Описание файлов

#### Дополнительные БД

* **dp_register.csv.gz** - реестр госпредприятий ([источник](http://www.spfu.gov.ua/ua/content/spf-stateproperty-Subiekti-gospodaruvannya.html))
* **population.csv** - население по областям на 01.04.2017 (Госстат)
* **postind.csv.gz** - БД почтовых индексов ([источник](http://data.gov.ua/passport/85f654c4-506a-497e-b80d-8879425e8f9c))
* **tax_deps.csv.gz** - список налоговых инспекций по областям ([источник](http://sfs.gov.ua/pro-sfs-ukraini/robota-z-personalom/pro-ochischennya/adresi-teritorialnih-dpi-gu-dfs-v-obl/))
* **vpp.csv.gz** - список крупных плательщиков налогов. Данные выборочные.

Крупные плательщики налогов в [реестре](https://github.com/woldemarg/tax_debt/tree/master/data_main) отнесены к *"офису крупных плательщиков налогов"*, поэтому определить область через список налоговых инспекций (**tax_deps.csv.gz**) для них нельзя.
