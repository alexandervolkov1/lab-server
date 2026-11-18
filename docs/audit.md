# Журнал управляющих операций

Журнал позволяет связать инициатора, запрос, допуск, вызов драйвера и сформированный
сервером результат. Это журнал фактов исполнения, а не подтверждение физического
состояния оборудования или доставки ответа клиенту. Чтения (`get`, `describe`,
`ping`, подписки, `server_status`, `components types/list/outputs`) не записываются.
API истории нет: просмотр выполняется SQLite-клиентом.

## Настройка

```toml
[recording]
enabled = true               # измерения и события агрегатора
audit_enabled = true         # управляющие операции; по умолчанию false
path = "measurements.sqlite"
queue_capacity = 1024        # очередь измерений; необязательно
audit_queue_capacity = 1024  # отдельная очередь аудита; необязательно
```

`enabled` сохраняет прежний смысл. Для одного аудита задайте `enabled = false`,
`audit_enabled = true`. Если оба флага false или секции нет, база не открывается.
Если включён хотя бы один режим, непустой `path` обязателен. Относительный путь
считается от рабочего каталога. Вместимости должны быть положительными.
Горячая перезагрузка не поддерживается. Открытие, проверка схемы, миграция и создание
сеанса происходят до открытия физических портов и запуска workers. Ошибка подготовки
базы фатальна для запуска. Оба режима используют один writer и один `sessions.id`.

## Идентификаторы и источники

Ключ операции — `(session_id, operation_id)`. `operation_id` — возрастающий u64,
сохраняемый как десятичный TEXT без потери точности. Он не равен `request_id`.
Счётчик общий для всех источников данного Recorder. Перезапуск создаёт новый сеанс.
Каждое событие повторяет неизменный `context_json`, поэтому потеря первого события
не лишает остальные сведений об инициаторе и запросе.

* `tcp` / `websocket`: `connection_id`, `request_id`, `request`. Соединения получают
  разные идентификаторы, в том числе при одинаковых request_id. Это **не пользователь**:
  аутентификации нет. При некорректном идентификаторе `request_id` может быть null.
  Ошибочный разбираемый JSON также сохраняется целиком в `invalid_frame`.
  Неразбираемый JSON и ошибки framing не классифицируются как операции управления.
* `controller`: `component`, `incarnation`, запрос с глобальным ключом и численной
  proposal PID. `requested.details.value` — фактическое типизированное значение
  после преобразования, например integer percent, переданное маршрутизатору.
* `internal`: создание/настройка компонентов из TOML, вызовы `DeviceService::manage`
  и автоматические переходы контроллеров в Fault, включая shutdown. У перехода
  сохраняются компонент, incarnation, причина, старое/новое состояние и ревизия.

У клиентских `set` сохраняются глобальный ключ и значение; у `invoke_action` —
глобальный ключ действия. У `components` сохраняется весь объект command:
действие, имя/конфигурация, параметры, входы, target привязки, expected_revision.
Удаление с cascade сохраняет запрос и результат управления. Manual — состояние
владения физическим выходом; отдельного нового состояния PID не вводится.

## События и причинность

| kind | Строгое значение |
|---|---|
| `requested` | PID преобразовал proposal в значение команды записи. Ещё не допуск. |
| `accepted` | Физическая команда попала в ограниченную очередь порта. Для синхронного управления компонентами — изменение принято и применено на границе сервиса. |
| `rejected` | Команда не допущена; сохранена ошибка валидации, маршрутизации, очереди или управления компонентом. |
| `started` | Пройдена существующая атомарная граница SetExecution/permit; следующая операция worker — вызов драйвера. |
| `completed` | Вызов драйвера вернулся. Сохраняются исходный успех либо сообщение DriverError и port_state, до преобразования в клиентский outcome_unknown. Перехваченная паника драйвера записывается как ошибка драйвера. |
| `cancelled` | Принятая команда закончилась до границы started: таймаут, отозванный permit, остановка порта или повторная валидация. Физический вызов не начался. |
| `applied` | Результат изменения обработки, revision_before/revision_after. Это не результат физического драйвера. Автоматический Fault не изменяет ревизию согласно прежним правилам Engine. |
| `response` | Сформирован JSON-результат для клиента, перед попыткой помещения в выходной буфер. Ни отправка, ни получение клиентом этим не подтверждаются. |
| `detached` | Сессия уничтожена с ожидающей операцией, для которой ответ не сформирован. Это не отменяет команду. |
| `controller_result` | Контроллер обработал результат Pending (включая timeout/outcome_unknown); отдельный факт от результата драйвера, без клиентского ответа. |

Если управление уже изменило ревизию, но построение результата завершилось ошибкой,
сохраняется `applied` с обеими ревизиями и ошибкой, затем ошибочный `response`.
Диагностические/read-only вызовы управления не создают аудит.

`sequence` начинается с 1 и назначается под коротким mutex одной операции **до**
попытки передачи. Пропущенные номера означают потери. `accepted` упорядочен с
worker через существующие границы порта и SetExecution; `completed` записывается
до передачи результата в reply channel. Параллельные `completed` и таймаутный
`response` могут иметь любой порядок, соответствующий фактическому наблюдению.
Нельзя выводить причинность разных операций из времени, rowid или порядка commit.
`registered_at_ns` — системное время Unix ns, оно может корректироваться ОС.

Пример позднего успеха:

```text
operation_id=17 sequence=1 accepted
operation_id=17 sequence=2 started
operation_id=17 sequence=3 response  {"id":8,"status":"error","error":"outcome_unknown"}
operation_id=17 sequence=4 completed {"status":"ok"}
```

Поздний успех не заменяет ответ и не порождает второй ответ. При раннем таймауте
последовательность: `accepted, cancelled, response(runtime_timeout)`; вызова драйвера
нет. При отключении клиента возможны `accepted, started, detached, completed`;
чистый EOF позволяет существующей сессии закончить обработку и сформировать ответ.
При отказе до допуска: `rejected, response`. При обычном успехе:
`accepted, started, completed, response`.

## SQLite и миграция

`PRAGMA application_id = 0x4c414252`, `user_version = 3`, WAL, synchronous FULL.
Миграции явные, транзакционные и добавочные: v1 → v2 (прежняя), затем
[`schema-v3.sql`](../src/recorder/schema-v3.sql) v2 → v3. Старые measurements,
events, record_order и sessions не удаляются и не перенумеровываются. У старых
сеансов audit_enabled=0: прошлое не объявляется аудированным. Неизвестная версия
или чужая/повреждённая схема отклоняется при подготовке.

Новые поля sessions: `audit_enabled`, `measurements_enabled`, `audit_incomplete`
(INTEGER), `audit_dropped_events` (десятичный TEXT u64).

```sql
CREATE TABLE audit_events (
 id INTEGER PRIMARY KEY,
 session_id INTEGER NOT NULL REFERENCES sessions(id),
 operation_id TEXT NOT NULL,
 sequence INTEGER NOT NULL,
 registered_at_ns INTEGER NOT NULL,
 kind TEXT NOT NULL,
 context_json TEXT NOT NULL,
 details_json TEXT NOT NULL,
 UNIQUE(session_id, operation_id, sequence)
) STRICT;
```

`record_order` сохраняет прежний смысл порядка измерений/событий агрегатора.
Аудит связан с ними общим session_id; глобальный порядок между очередями не задан.
Числа значений JSON сохраняют тип, i64 не преобразуется во float. Счётчики u64 и
идентификаторы в контексте используют строки, ревизии JSON — точные целые числа.

## Неполнота, нагрузка и отказ

Очереди раздельные: измерения не занимают места аудита. Передача — `try_send`,
никаких SQLite-вызовов или ожиданий места в Session, PID, PortWorker. Writer
выбирает максимум одно событие каждой очереди за оборот, сбрасывает оба пакета,
когда один достиг batch_size (по умолчанию 128), либо по интервалу (100 ms).
Ожидание idle writer ограничено интервалом, но время файлового I/O не гарантируется.
Такой выбор не создаёт голодания ни одной очереди. В памяти — две ограниченные
очереди и два ограниченных пакета, контекст разделяется через Arc.
Размер context_json + details_json одного события ограничен 128 KiB. Более крупное
событие отбрасывается целиком с учётом потери, без незаметного усечения.

`dropped_audit_events` — отдельный накопительный счётчик: переполнение, слишком
большие данные/непредставимое время, закрытая/отказавшая очередь и все незафиксированные
события при отказе writer. Последующие попытки после отказа также учитываются.
Насыщение u64 означает «не меньше MAX», счётчик никогда не сбрасывается чтением.
Счётчик виден в `server_status.recorder.losses`, итоговом Audit summary, снимке
RecorderDataLost и sessions.audit_dropped_events при успешной записи снимка.

Новый сеанс с аудитом сразу получает **audit_incomplete=1**. Только штатное
завершение после остановки производителей, полного drain и commit без потерь
снимает этот флаг. Полнота относится к сохранению сформированных событий, а не
к обязательному наличию результата у каждой принятой операции. Пока сервер работает, это означает «полнота ещё не подтверждена».
При аварии/ошибке SQLite флаг остаётся выставлен, даже если обновить счётчики
невозможно. Последний сохранённый счётчик тогда лишь нижняя граница; фактические
потери доступны в памяти и итоговой диагностике. Отсутствие ended_at_ns также
означает незавершённый сеанс. Внешний код, использующий Recorder отдельно, обязан
остановить производителей до shutdown; попытки через пережившие его handles уже
не могут обновить закрытую базу.

Ошибка SQLite после запуска оставляет физическое управление работающим. Нет
повторов физических команд, повторного допуска или остановки оборудования ради
аудита. Ошибка Recorder сохраняется до shutdown и возвращается итоговым результатом
сервера. При аварии процесса теряются очереди/незакоммиченные пакеты; отсутствие
события не доказывает отсутствие физического действия. `completed(ok)` сообщает
успех драйвера, а не независимо измеренный физический эффект. Прямые низкоуровневые
вызовы PortHandle/CommandRouter вне DeviceService не приобретают инициатора
автоматически; production TCP/WS/PID проходят инструментированные пути сервиса.

## Примеры просмотра

```sql
-- Сеансы, полнота и известные потери. Сначала проверяйте эти поля.
SELECT id, started_at_ns, ended_at_ns, audit_enabled, audit_incomplete,
       audit_dropped_events FROM sessions ORDER BY id DESC;

-- Хронология одной операции: подставьте session_id и строковый operation_id.
SELECT sequence, kind, registered_at_ns, context_json, details_json
FROM audit_events WHERE session_id=1 AND operation_id='17' ORDER BY sequence;

-- Запросы клиентов и контроллеров (даже если начальное событие потерялось).
SELECT operation_id, json_extract(context_json,'$.source') AS source,
       json_extract(context_json,'$.connection_id') AS connection_id,
       json_extract(context_json,'$.request_id') AS request_id,
       json_extract(context_json,'$.component') AS component,
       json_extract(context_json,'$.request') AS request
FROM audit_events WHERE session_id=1 GROUP BY operation_id;

-- Клиентский unknown с отдельным поздним успешным результатом драйвера.
SELECT r.operation_id, r.sequence AS response_seq, d.sequence AS driver_seq
FROM audit_events r JOIN audit_events d
 ON r.session_id=d.session_id AND r.operation_id=d.operation_id
WHERE r.kind='response' AND json_extract(r.details_json,'$.error')='outcome_unknown'
 AND d.kind='completed' AND json_extract(d.details_json,'$.status')='ok'
 AND d.sequence>r.sequence;

-- Изменения обработки и ревизии.
SELECT operation_id, context_json,
       json_extract(details_json,'$.revision_before') AS before,
       json_extract(details_json,'$.revision_after') AS after, details_json
FROM audit_events WHERE kind='applied' ORDER BY id;
```

## Проверки

`tests/audit.rs`: ручной set, action, ошибки допуска/драйвера, отмена, timeout после
старта, поздний успех, уничтожение сессии, ошибка SQLite без остановки/повторов.
`tests/processing_service.rs`: запись PID с incarnation, отмена/ошибка PID,
параметры, конфигурация, удаление, Shadow/Auto и ревизии.
`tests/websocket.rs`: реальные TCP/WS соединения с различными connection_id.
`src/recorder/tests.rs`: независимые ограниченные очереди, потери, drain, миграции.
`tests/startup/recording.rs`: четыре комбинации флагов, ошибка открытия до оборудования.
Все используют имитационные драйверы. Для общего набора тестов задайте
`CARGO_TARGET_DIR` (например абсолютный путь к `target` репозитория).
