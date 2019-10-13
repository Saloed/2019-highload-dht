# 2019-highload-dht
Курсовой проект 2019 года [курса](https://polis.mail.ru/curriculum/program/discipline/792/) "Highload системы" в [Технополис](https://polis.mail.ru).

## Этап 1. HTTP + storage (deadline 2019-10-04)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2019-highload-dht.git
Cloning into '2019-highload-dht'...
...
$ git remote add upstream git@github.com:polis-mail-ru/2019-highload-dht.git
$ git fetch upstream
From github.com:polis-mail-ru/2019-highload-dht
 * [new branch]      master     -> upstream/master
```

### Make
Так можно запустить тесты:
```
$ gradle test
```

А вот так -- сервер:
```
$ gradle run
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx128m`. 

В своём Java package `ru.mail.polis.service.<username>` реализуйте интерфейс [`Service`](src/main/java/ru/mail/polis/service/Service.java) и поддержите следующий HTTP REST API протокол:
* HTTP `GET /v0/entity?id=<ID>` -- получить данные по ключу `<ID>`. Возвращает `200 OK` и данные или `404 Not Found`.
* HTTP `PUT /v0/entity?id=<ID>` -- создать/перезаписать (upsert) данные по ключу `<ID>`. Возвращает `201 Created`.
* HTTP `DELETE /v0/entity?id=<ID>` -- удалить данные по ключу `<ID>`. Возвращает `202 Accepted`.

Возвращайте реализацию интерфейса в [`ServiceFactory`](src/main/java/ru/mail/polis/service/ServiceFactory.java).

Реализацию `DAO` берём из весеннего курса `2019-db-lsm`, либо запиливаем [adapter](https://en.wikipedia.org/wiki/Adapter_pattern) к уже готовой реализации LSM с биндингами на Java (например, RocksDB, LevelDB или любой другой).

Проведите нагрузочное тестирование с помощью [wrk](https://github.com/giltene/wrk2) в **одно соединение**.
Почему не `curl`/F5, можно узнать [здесь](http://highscalability.com/blog/2015/10/5/your-load-generator-is-probably-lying-to-you-take-the-red-pi.html) и [здесь](https://www.youtube.com/watch?v=lJ8ydIuPFeU).

Попрофилируйте (CPU и alloc) под нагрузкой с помощью [async-profiler](https://github.com/jvm-profiling-tools/async-profiler) и проанализируйте результаты.

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/).
Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией и оптимизациями на review.
Не забывайте **отвечать на комментарии в PR** (в том числе автоматизированные) и **исправлять замечания**!

## Этап 2. Многопоточность (deadline 2019-10-11)

Обеспечьте потокобезопасность реализации `DAO` с помощью `synchronized`, а лучше -- с использованием примитивов `java.util.concurrent.*`.
Прокачаться можно с руководством [Java Concurrency in Practice](http://jcip.net/).

Сконфигурируйте HTTP сервер, чтобы он обрабатывал запросы с помощью пула из нескольких потоков.

Проведите нагрузочное тестирование с помощью [wrk](https://github.com/giltene/wrk2) в **несколько соединений**.

Отпрофилируйте приложение (CPU, alloc и **lock**) под нагрузкой с помощью [async-profiler](https://github.com/jvm-profiling-tools/async-profiler) и проанализируйте результаты.

### Report
Когда всё будет готово, присылайте pull request со своей реализацией и оптимизациями на review.

## Этап 3. Асинхронный сервер (deadline 2019-10-19)

Реализуйте асинхронный HTTP сервер на основе [one-nio](https://github.com/odnoklassniki/one-nio).

Проведите нагрузочное тестирование с помощью [wrk](https://github.com/giltene/wrk2) в **несколько соединений** с разными видами запросов.

Попрофилируйте приложение (CPU, alloc и lock) под нагрузкой с помощью [async-profiler](https://github.com/jvm-profiling-tools/async-profiler) и проанализируйте результаты.

Реализуйте получение **диапазона данных** с помощью HTTP `GET /v0/entities?start=<ID>[&end=<ID>]`, который возвращает:
  * Статус код `200 OK`
  * Возможно пустой **отсортированный** (по ключу) набор **ключей** и **значений** в диапазоне ключей от **обязательного** `start` (включая) до **опционального** `end` (не включая)
  * Использует [Chunked transfer encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding)
  * Чанки в формате `<key>\n<value>`

Диапазон должен отдаваться в **потоковом режиме** без формирования всего ответа в памяти.

### Report
Когда всё будет готово, присылайте pull request с изменениями и результатами нагрузочного тестирования и профилирования.
