# Руководство пользователя lab-server

lab-server принимает измерения от приборов, выполняет граф обработки и обслуживает локальных клиентов.
Графики, Steel REPL и сценарии находятся на стороне lab-plot. Сценарии отправляют серверу обычные TCP-команды.

## Сборка и запуск

Нужен Rust с Cargo и поддержкой редакции 2024. Под Windows используйте Rust toolchain и соответствующие инструменты сборки.
Соберите сервер и запустите виртуальный эксперимент из корня репозитория.

~~~powershell
cargo build --release
cargo run --release -- --config examples/virtual-plant-external.toml
~~~

Аргумент `--config` обязателен. Путь с пробелами заключайте в кавычки.
Сервер печатает фактические адреса TCP и WebSocket при запуске.

| Конфигурация | Назначение |
| --- | --- |
| [virtual-plant-external.toml](../examples/virtual-plant-external.toml) | ThermalPlant, внешний граф EMA/PID, SQLite и аудит |
| [virtual-plant.toml](../examples/virtual-plant.toml) | ThermalPlant и встроенные компоненты |
| [arduino.toml](../examples/arduino.toml) | Физическая Arduino |
| [processing.toml](../examples/processing.toml) | Arduino с EMA и PID |
| [metakon-readonly.toml](../examples/metakon-readonly.toml) | Первый запуск МЕТАКОН в режиме чтения |
| [websocket.toml](../examples/websocket.toml) | Проверка обоих транспортов с пустым каталогом |

## Файлы конфигурации и приборы

В основном TOML секция `[tcp]` задаёт `bind`, обязательный список `[[devices]]` описывает экземпляры приборов.
Для пустого каталога задайте `devices = []`. `[[ports]]` описывает физические соединения,
`[[virtual_ports]]` — модели. `[websocket]`, `[recording]` и `[processing]` задаются по необходимости.
Ключ сигнала имеет форму `device_id/local_parameter`, например `furnace1/sample_temperature`.

Для физического подключения настройте `name`, `baud`, `data_bits`, `parity`, `stop_bits`, `flow_control` и `timeout_ms`.
В устройстве укажите `driver` и ссылку `port` на `id` соединения. Один физический порт можно разделить
между совместимыми приборами. Необязательная таблица `[devices.poll_periods_ms]` переопределяет
периоды опроса по локальным именам сигналов, в миллисекундах.

- **Arduino.** Проверьте фактический COM-порт и параметры линии. Шаблон использует 57600 бод и пустые `options`.
- **МЕТАКОН.** Проверьте COM-порт, скорость, адрес, канал с отсчётом от нуля, масштаб и единицы.
  Оставьте `write_enabled = false` для проверки чтения. Автоматические тесты используют
  эмулятор. Подключение и поведение прибора требуют отдельной проверки.
  [Инструкция проверки на физическом приборе](metakon-hardware-test.md).
- **ThermalPlant.** В `[[virtual_ports]]` задайте `model = "thermal_plant"` и при необходимости `seed`
  в `[virtual_ports.options]`. Привяжите один прибор с `driver = "virtual"`. Модель выдаёт
  `heater_temperature`, `sample_temperature` и `room_temperature` в °C и принимает целое `power` от 0 до 100%.
  Начальная мощность модели — 0%.

Ошибка конфигурации прерывает запуск. При недоступном физическом порте сервер может запуститься,
сохранив прибор в каталоге. Автоматического переподключения COM-портов нет. Устраните причину и явно перезапустите сервер.

Граф Processing можно объявить в `[[processing.components]]` либо вынести в отдельный файл.

~~~toml
[processing]
file = "experiments/thermal-loop.toml"
~~~

Относительный путь отсчитывается от каталога основного TOML. Абсолютный путь тоже допустим.
Внешний файл содержит `schema_version = 1` и массив `[[components]]`, а привязку задаёт `[components.binding]`.
Пример с обоими компонентами есть в [thermal-loop.toml](../examples/experiments/thermal-loop.toml).
Сочетание `file` и встроенных компонентов отклоняется. Для эксперимента, который вы будете сохранять,
скопируйте основной TOML вместе с подкаталогом `experiments` в рабочий каталог опыта.

## Подключение и команды

TCP использует UTF-8 NDJSON. Отправляйте один JSON-объект на строку с LF, допускается CRLF.
Для WebSocket включите `[websocket]` с `enabled = true` и `bind`. Подключайтесь к
`ws://127.0.0.1:7241/application/v1` с подпротоколом `lab-server.application.v1`.
Передавайте один JSON на текстовое сообщение. Лимит входного сообщения — 16 КиБ.
Оба транспорта слушают только числовые loopback-адреса и работают без клиентской авторизации и TLS.
Если клиент передаёт `Origin`, он должен совпасть с записью `allowed_origins`. `Origin` не удостоверяет клиента.

Команды PowerShell отправляют `hello` по TCP и печатают ответ.

~~~powershell
$client = [System.Net.Sockets.TcpClient]::new('127.0.0.1', 7240)
try {
    $stream = $client.GetStream()
    $stream.ReadTimeout = 5000
    $request = [System.Text.Encoding]::UTF8.GetBytes('{"id":1,"op":"hello"}' + [char]10)
    $stream.Write($request, 0, $request.Length)
    $reader = [System.IO.StreamReader]::new($stream)
    $reader.ReadLine()
} finally {
    $client.Dispose()
}
~~~

Каждый запрос содержит `op` и положительный целый `id` в диапазоне `u64`. ID строго растут в рамках соединения.
Сопоставляйте ответы по `id`. События подписок приходят отдельно без `id`.

| Операция | Пример запроса и результат |
| --- | --- |
| `hello` | `{"id":1,"op":"hello"}` → `protocol = "lab-server"`, `version = 1` |
| `ping` | `{"id":2,"op":"ping"}` → ACK |
| `describe` | `{"id":3,"op":"describe"}` → массивы `parameters` и `actions` |
| `server_status` | `{"id":4,"op":"server_status"}` → `data` со статусом портов, приборов, Processing и Recorder |
| `get` | `{"id":5,"op":"get","key":"furnace1/sample_temperature"}` → значение или `no_data` |
| `set` | `{"id":6,"op":"set","key":"furnace1/power","value":20}` → ACK для разрешённой записи |
| `subscribe` | `{"id":7,"op":"subscribe","keys":["furnace1/sample_temperature"]}` → полный набор подписанных ключей |
| `unsubscribe` | `{"id":8,"op":"unsubscribe","keys":["furnace1/sample_temperature"]}` → оставшиеся ключи |
| `invoke_action` | `{"id":9,"op":"invoke_action","key":"furnace1/apply"}` → действие Arduino, если оно доступно в `describe` |

`get` периодического сигнала читает кеш. Для сигнала без опроса сервер запрашивает прибор.
На подписку доступны опрашиваемые сигналы и опубликованные выходы Processing. После ACK поступают новые
наблюдения. `completed_at_unix_ms` — время завершения измерения, в миллисекундах Unix.

~~~json
{"id":5,"status":"ok","key":"furnace1/sample_temperature","value":20.0,"unit":"degC","completed_at_unix_ms":1700000000000}
{"event":"measurement","status":"ok","key":"furnace1/sample_temperature","value":20.1,"unit":"degC","completed_at_unix_ms":1700000001000}
~~~

Перед записью сверьте `writable`, `type`, `min` и `max` через `describe`. Для `integer` нужен целочисленный
литерал, для `float` — дробный или экспоненциальный, для `boolean` — `true` или `false`.
ACK `{"id":6,"status":"ok"}` подтверждает успешное завершение транзакции по контракту драйвера.
Физический эффект отдельно проверяет оператор. Используйте `invoke_action` только для действий из каталога,
с учётом последствий для прибора. [Полный протокол](protocol.md) · [Диагностика](server-status.md)

## Компоненты и ревизии

Для всех операций передавайте `op = "components"` и объект `command` с полем `action`.

| `action` | Поля `command` и результат |
| --- | --- |
| `types` | Схемы типов, входов, выходов и параметров по умолчанию |
| `list` | Необязательные `offset` и `limit` (1–64). Текущая `revision`, `instances` и `next_offset` |
| `create` | Полная `configuration` и `expected_revision` |
| `configure` | Полная новая `configuration` и `expected_revision`. Пропущенные параметры получают значения по умолчанию |
| `parameter_set` | `name`, `parameter`, `value` и необязательная `expected_revision`. Меняет один параметр |
| `delete` | `name`, `expected_revision` и необязательное `cascade`. Каскад удаляет зависимые компоненты |
| `bind` | `name`, `target`, `command_timeout_ms` (1–10000) и `expected_revision`. Контроллер должен быть в Shadow |
| `state` | `name`, `state` (`shadow`, `stopped` или `auto`) и `expected_revision` |
| `outputs` | Режим и владелец доступных для записи выходов, `in_flight` и `outcome_unknown` |
| `save` | Сохраняет внешний TOML. В `data` возвращается ревизия снимка |

В `configuration` обязательны `name`, `type_id`, `inputs` и `max_age_ms`, `parameters` можно опустить.
Успешная мутация возвращает снимок с новой `revision`. Используйте её для следующего
`expected_revision`. Если клиент получил `revision_conflict` или ревизии страниц различаются,
перечитайте `list` и проверьте, применима ли правка. Save использует текущую ревизию без её увеличения.

Свежий внешний ThermalPlant из примера начинает с `revision = 3`. После запуска запросы можно отправить в таком порядке.

~~~json
{"id":10,"op":"components","command":{"action":"list"}}
{"id":11,"op":"components","command":{"action":"create","configuration":{"name":"smooth2","type_id":"ema","inputs":{"input":"furnace1/sample_temperature"},"parameters":{"tau":2.0},"max_age_ms":5000},"expected_revision":3}}
{"id":12,"op":"components","command":{"action":"parameter_set","name":"loop","parameter":"setpoint","value":45.0,"expected_revision":4}}
{"id":13,"op":"components","command":{"action":"save"}}
~~~

После успеха ожидайте `{"id":13,"status":"ok","data":{"revision":5}}`.
Save заменяет внешний файл, сохраняет граф, параметры и привязки. Режим Auto и историю вычислений
он не сохраняет. Для встроенной конфигурации возвращается `no_experiment_file`. Сервер не отслеживает
автоматически правки файла, внесённые другим процессом. [Полный API компонентов](processing-api.md)

### EMA, PID и выходы

EMA в Running сглаживает вход с постоянной времени `tau` в секундах. Выход примера — `processing/smooth/value`.
PID в Shadow рассчитывает предложение `processing/loop/output`, сохраняя выход Manual. Уставка
видна как `processing/loop/setpoint` в единицах входа. Сервер проверяет параметры и зависимости
графа при запуске и при изменениях конфигурации.

Auto включайте явным `state` только при готовом свежем входе и проверенной привязке к числовому
`writable`-выходу. Для пробы пользуйтесь ThermalPlant. В Auto данный выход принадлежит контроллеру,
и ручной `set` получает `output_owned`. Для изменения Auto-контроллера сначала верните его в Shadow.
При Fault выясните причину, верните PID в Shadow и дождитесь свежего входа перед возможным Auto.
Shadow, освобождение выхода и остановка сервера не гарантируют нулевой физической мощности.

После Save с ревизией 5 дождитесь свежего входа. На ThermalPlant можно включить и остановить Auto следующими запросами.

~~~json
{"id":14,"op":"components","command":{"action":"state","name":"loop","state":"auto","expected_revision":5}}
{"id":15,"op":"components","command":{"action":"state","name":"loop","state":"stopped","expected_revision":6}}
~~~

## SQLite, ошибки и завершение

В `[recording]` задайте `enabled = true` для измерений, `audit_enabled = true` для аудита
и `path` для файла SQLite. Относительный путь SQLite отсчитывается от рабочего каталога процесса.
Каждый запуск создаёт сеанс. Таблица `measurements` хранит исходные и производные измерения,
`audit_events` — команды и результаты, включая Save. Для состояния очередей и потерь используйте
`server_status`, а после остановки проверьте итоговые сообщения Recorder. [Схема и аудит](audit.md)

| Ответ | Что проверить |
| --- | --- |
| `no_data` | Дождитесь первого успешного опроса или вычисления |
| `unknown_key`, `not_subscribable` | Проверьте ключ, каталог и период опроса |
| `runtime_unavailable` | Посмотрите состояние порта в `server_status` и stderr |
| `runtime_busy`, `too_many_pending` | Уменьшите нагрузку и дождитесь ответов на уже принятые запросы |
| `runtime_timeout` | Для `set` и `invoke_action` исполнение не началось в отведённый срок |
| `outcome_unknown` | Исход записи или действия неизвестен. Проверьте фактическое состояние и не повторяйте автоматически |
| `observation_data_lost`, `observation_stream_closed` | Откройте новое соединение и подпишитесь заново. Потерянные измерения не восстанавливаются |
| `revision_conflict`, `experiment_write_failed` | Перечитайте снимок или проверьте возможность записи внешнего файла |

Перед остановкой приведите реальную установку в безопасное состояние по её штатной процедуре.
Сервер не выполняет автоматическую запись безопасной мощности. Нажмите `Ctrl+C` в консоли
и дождитесь выхода процесса и закрытия SQLite. При включённой записи проверьте итог и признак
`incomplete`. При следующем запуске PID создаётся в Shadow, выход остаётся Manual.
