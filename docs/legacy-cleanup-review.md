# Legacy cleanup: материал для review

Ветка refactor/legacy-cleanup создана от eee832ea655ed61122958d782bb07d4fcee54f36.
Локальный и origin/master перед работой: abfd1cb19f528c6c89dfafb6b454a475fbf937db.
Коммит предыдущей миграции eee832e не копировался. Слияние в master не выполняется.

## Область изменений и риски

| Этап / файлы | Изменение и причина | Риск поведения / проверка |
| --- | --- | --- |
| 1: connection/tests/boolean.rs | Fixture запускает startup::start_with/run_until с BooleanDriver и DeviceIo в памяти. Реальные DeviceService, Router, PortWorker и TCP. Все ключи глобальные; драйвер проверяет локальное enabled. | Тестовая модель отказа ACK явно Synchronized: прежний worker игнорировал состояние порта, современный останавливается при Desynchronized. Сохраняется проверка эффекта записи после outcome_unknown; физические отказы покрыты port_worker. |
| 2: connection/tests/streaming.rs, fanout/test_support.rs | Routed fanout, настоящий реестр/handles, TestBackend для команд. Не запущенные workers удерживают каталоги; публикации и агрегированные потери задаются управляемо. | Не заменяется физическая гонка: её отдельные тесты subscription_races используют настоящий worker и барьеры. Сохранены все 13 сценариев, добавлены изоляция портов и обычный EOF. |
| 3: tests/transport_session.rs, connection/tests/session.rs | Публичный DeviceService с реальным PortWorker и контролируемым драйвером. Канал разрешает завершение Set, последующее чтение служит барьером завершения. | Начало подтверждается входом в драйвер. В EOF-тесте физический FIFO означает: id=1 начат, id=2 ожидает (в старом стенде наоборот). Проверяются оба результата и отсутствие вызова отменённого Set. Освобождение reply receiver проверяется отдельно unit-тестом. |
| 4: runtime.rs, tcp_legacy.rs, lib.rs, session.rs, pending.rs, fanout.rs | Удалена невостребованная реализация и старые completion-ветви. Routed fanout конструируется непосредственно. | Преднамеренное удаление Rust API перечислено ниже. В routed ветке сохранены порядок блокировок, атомарные Ordering, refresh до смены подписки, sticky-loss, насыщение и приоритет потери над EOF. |
| 4: tests/port_worker.rs | Существующий тест постоянной нагрузки дополнен on-demand Read наряду с Set. | Проверяется чередование 8 команд и 8 опросов, равномерность двух опрашиваемых устройств, ответы и отсутствие дополнительных наблюдений от команд. Scheduler не изменён. |
| 5: session/requests.rs, session/subscriptions.rs, protocol/tests.rs | Перенесены отслеживание/кодирование завершений, атомарная проверка набора подписок и блок unit-тестов parser. | Реэкспорты сохраняют внешние пути. Только внутренняя видимость contains_id/completion_json — pub(super); command_error_code стал private. Поведение проверено полным набором. |
| 6: README, docs, четыре Steel .scm | Удалены временные этапы и противоречивые примеры. Краткий справочник остаётся в README; детали сохранены в architecture/protocol. Steel привязан к furnace1 из Arduino TOML. | JSON, локальные ссылки, конфигурационные тесты и Steel AST проверяются без оборудования. В config/protocol изменены только устаревшие комментарии. |

## Устаревшая часть и публичный Rust API

| Часть | Результат |
| --- | --- |
| runtime.rs / pub mod runtime | Удалены. Включая DeviceCommand, SetCommand, PollTask, ObservationOutput, run_polling и реэкспорты двух констант очередей из этого модуля. Оригиналы констант port_worker сохранены. |
| tcp_legacy.rs; корневые queue_read/queue_set | Удалены вместе с реализацией CommandBackend для SyncSender старого DeviceCommand. |
| PendingRequests::add_read/add_set | Удалены. Routed pending используется сервисом и cfg(test) TestBackend; публичный API ради тестов не добавлялся. |
| FinishedRequest::Read/Set | Удалены. RoutedRead/RoutedMutation и пути session/connection сохранены. |
| ObservationFanout::new/loss_epoch, ObservationInterest::set_active | Удалены. Остаются routed, set_keys, пер-портовые счётчики и причина потери. Старый глобальный счётчик и Option-режим domains удалены. |
| fanout.rs, Session/ClientSession, TcpConnection, CommandBackend, SetExecution | Сохранены как рабочие компоненты. |
| PortWorker, DeviceRegistry, ObservationAggregator, Recorder, DeviceService | Сохранены; scheduler, драйверы, форматы JSON/SQL и порядок shutdown не менялись. |
| subscribe key/keys; Legacy в generic_actions.rs | Сохранены: действующая совместимость JSON и тест драйвера с пустым каталогом действий. |

Эти удаления — несовместимое изменение библиотечного Rust API, даже при неизменном TCP/WS API v1.
Сторонние потребители за пределами репозитория статическим поиском не проверяются; это предмет итогового согласования.
Новых публичных точек для тестов нет. Произвольный внешний CommandBackend не получает доступа к внутреннему
add_routed_*: расширение этого API в задачу не включено.

## Соответствие перенесённых тестов

Во всех строках, кроме отдельно указанной, имя и исходные проверки сохранены. Тесты не удалены как «неприменимые».

| Старый файл / тест | Новый путь / покрытие |
| --- | --- |
| src/connection/tests/boolean.rs: `boolean_tcp_describe_set_and_get_preserve_types` | тот же файл; production startup и BooleanDriver |
| src/connection/tests/boolean.rs: `boolean_tcp_rejects_invalid_writes_before_driver_and_consumes_ids` | тот же файл; production startup и BooleanDriver |
| src/connection/tests/boolean.rs: `boolean_tcp_subscription_ack_precedes_polled_values_and_read_errors` | тот же файл; production startup и BooleanDriver |
| src/connection/tests/boolean.rs: `boolean_tcp_write_failure_reports_unknown_outcome` | тот же файл; production startup и BooleanDriver |
| src/connection/tests/streaming.rs: `subscribe_excludes_old_publications_and_orders_ack_first` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_disconnect_releases_slot_for_reconnection` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_reports_upstream_data_loss` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `upstream_loss_survives_unsubscribe_and_resubscribe_before_notification` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `personal_loss_survives_unsubscribe_and_resubscribe_before_notification` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `observation_loss_preserves_command_only_client_and_pending_set` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `loss_while_unsubscribed_does_not_poison_later_subscription` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `subscribed_client_loss_does_not_fabricate_pending_set_result` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `subscription_boundaries_are_per_key_and_per_client` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_streams_only_subscribed_observations` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_unsubscribe_stops_measurements_after_ack` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_closes_after_observation_channel_overflow` | тот же файл; routed fanout + TestBackend |
| src/connection/tests/streaming.rs: `tcp_run_combines_commands_and_observation_stream` | тот же файл; routed fanout + TestBackend |
| tests/transport_session.rs: `complete_json_text_has_no_ndjson_dependency_and_uses_the_existing_parser` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `message_queue_keeps_utf8_boundaries_and_the_exact_64k_lf_budget` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `tcp_compatibility_framing_keeps_valid_prefix_before_terminal_error` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `tcp_partial_writes_release_only_written_bytes_and_cannot_replay_a_prefix` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `output_overflow_preserves_queued_prefix_and_terminates_message_session` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `concurrent_message_session_orders_subscription_ack_completion_and_measurement` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `unsubscribe_and_reactivation_exclude_queued_old_publications` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `message_receiver_overflow_is_sticky_across_resubscription` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `orderly_observation_close_drains_prefix_and_differs_from_data_loss` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `observation_service_is_bounded_to_one_turn` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `concurrent_message_eof_preserves_queued_cancellation_and_started_outcome_unknown` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `clean_message_eof_keeps_final_ack_and_stops_observation_admission` | тот же файл; публичный DeviceService + PortWorker |
| tests/transport_session.rs: `terminal_loss_does_not_fabricate_pending_ack_and_drop_releases_reply_receiver` | terminal_loss_does_not_fabricate_pending_ack_or_block_worker_after_drop + unit dropping_terminal_session_releases_started_mutation_reply_receiver |
| src/fanout.rs: `loss_before_first_subscription_does_not_poison_subscription` | тот же файл; routed fanout |
| src/fanout.rs: `upstream_loss_closes_existing_streams_and_allows_reconnect` | тот же файл; routed fanout |
| src/fanout.rs: `disconnected_registrations_do_not_accumulate_without_publications` | тот же файл; routed fanout |
| src/fanout.rs: `publishes_to_multiple_clients` | тот же файл; routed fanout |
| src/fanout.rs: `slow_client_does_not_block_other_clients` | тот же файл; routed fanout |
| src/fanout.rs: `personal_overflow_persists_until_receiver_is_replaced` | тот же файл; routed fanout |

Дополнительно в streaming: other_port_publications_and_loss_do_not_overflow_subscribed_client и
orderly_port_eof_drains_queued_observation_without_reporting_loss. Первый проверяет и поток A, и loss A при подписке только B.
Два исходных теста upstream_loss_survives_unsubscribe_and_resubscribe_before_notification и
personal_loss_survives_unsubscribe_and_resubscribe_before_notification не делают клиентский проход между потерей и сменой подписки.
35 тестов protocol перенесены без изменения в src/protocol/tests.rs; тесты session/pending/tcp/subscriptions предыдущих этапов сохранены.

## Все удалённые тесты и замена гарантии

Удалены только следующие 11 функций вместе с runtime.rs. Это проверки удалённой реализации;
одноимённый legacy worker не оставлен ради тестового транспорта.

| Удалённый runtime::tests | Современное покрытие |
| --- | --- |
| `upstream_overflow_is_nonblocking_and_signals_loss` | port_worker: observation_overflow_is_explicit_and_never_blocks_worker; observation_overflow_is_local_to_one_worker |
| `driver_failure_after_start_reports_unknown_outcome` | port_worker: every_started_set_driver_error_is_outcome_unknown; boolean_tcp_write_failure_reports_unknown_outcome |
| `expired_queued_set_never_reaches_driver` | port_worker: expired_and_cancelled_sets_never_reach_driver; transport_session: concurrent_message_eof_preserves_queued_cancellation_and_started_outcome_unknown |
| `started_set_timeout_is_unknown_and_has_no_second_reply` | port_worker: started_set_timeout_keeps_port_owned_until_late_reply_finishes; session: started_mutation_timeout_has_unknown_outcome_and_no_second_reply |
| `timely_set_reply_is_delivered_once` | session: session_encodes_completed_read_and_set; transport_session: concurrent_message_session_orders_subscription_ack_completion_and_measurement |
| `preserves_polling_phase` | port_worker::scheduler: periods_are_independent_and_missed_ticks_do_not_accumulate |
| `schedules_independed_signals` | port_worker::scheduler: periods_are_independent_and_missed_ticks_do_not_accumulate; equal_deadline_groups_rotate_the_first_device (современная справедливость вместо старого порядка single worker) |
| `processes_set_during_overloaded_polling` | port_worker: commands_and_overdue_polls_alternate_under_sustained_load, ветвь Set |
| `polling_continues_with_queued_writes` | тот же тест: непрерывная очередь 8 Set и ровно 8 чередующихся опросов двух приборов |
| `reads_on_demand_during_overloaded_polling` | тот же тест: добавленная ветвь Read отдельного неопрашиваемого прибора, завершение всех 8 запросов |
| `handles_commands_without_polling_tasks` | port_worker: different_drivers_share_one_io_with_independent_state_and_local_names; idle_worker_stops_on_shutdown_or_last_sender_drop |

## Проверки и архитектурная оценка

| Контрольная точка | Debug | Release | fmt/check/Clippy -D warnings |
| --- | ---: | ---: | --- |
| Исходное состояние | 343 | 343 | успешно |
| Boolean | 343 | не требовался на этом шаге | успешно |
| Streaming | 345 | 345 | успешно |
| Transport session | 346 | 346 | успешно |
| Удаление legacy | 335 | 335 | успешно |
| Структура | 335 | 335 | успешно |
| Документация / финальный полный набор | 335 | 335 | успешно |

Итоговое изменение числа тестов: 343 + 2 регрессии потоков + 1 проверка освобождения receiver − 11 runtime = 335.
Полезные прежние сценарии сохранены, тесты не добавлялись для компенсации числа.

TCP/WS parity, hello.version, ключи с вложенным /, строгие integer/float/boolean и пределы i64 защищают
protocol, model, device_registry_router, tcp_router, transport_session, websocket, generic_actions и arduino_designer.
Pending, session и port_worker проверяют ровно одно завершение, timeout/OutcomeUnknown, отсутствие retry и границу try_start.
Fanout, streaming, tcp_router, observation_aggregator и subscription_races проверяют фильтрацию до очереди,
активный sticky-loss, независимые порты, задержанный DataLost, насыщение u64::MAX и отличие EOF.
Startup/Recorder и тесты TCP/WS+SQLite проверяют дренирование до EOF, финальный shutdown, транзакции,
типы данных, ошибки writer/закрытия и их сохранение. Существующие пользовательские SQLite не открывались.

Исправления, которые были необходимы: зависимости тестов от другого механизма исполнения,
двойные ветви completion/fanout, устаревшие инструкции и неработающие короткие ключи примеров.
Разделение файлов — улучшение навигации, не исправление ошибки поведения.
Session остаётся владельцем dispatch и границ наблюдений; их дополнительное дробление разнесло бы инварианты.
Startup (~600 строк) оставлен цельным: небольшая Recording-обвязка тесно связана с dispatch, завершением
и ServerErrors; перенос потребовал бы дополнительной внутренней видимости без ощутимого выигрыша.
PortWorker, Recorder и Arduino уже разделены; их организация не менялась ради одинакового размера файлов.

Все зависимости Cargo.toml имеют реальные потребители; удалений и новых зависимостей нет.
Общие тестовые механизмы переиспользованы: TestBackend и routed_fanout. Драйверы разных тестов сохраняют
специфические управляемые отказы/барьеры; универсальный тестовый транспорт не введён.
Публичные совместимые обёртки (например dispatch_describe, TcpRequest, MultiDeviceTcp) сохранены,
даже если внутренний путь использует более общий вариант: отсутствие внутренних вызовов не доказывает ненужность API.

Оставшиеся ограничения: блокирующий драйвер обязан иметь конечный I/O timeout; состояние транзакции определяется
контрактом DriverError, автоматического восстановления нет. EOF и успешный Recorder::shutdown не доказывают
отсутствие прошлых потерь. Граница публикации fanout не является временем физического измерения.
Тесты с имитацией не заменяют проверку прошивки/оборудования; Arduino и МЕТАКОН в этой работе не подключались.
Продолжение рефакторинга или новые функции в эти ограничения не включены.

## Проверка документации и границ diff

61 JSON-пример проверен на синтаксис и глобальные key/keys; 27 запросов дополнительно прошли
production parse_request из собранной библиотеки. Все три TOML прочитаны ServerConfig::load,
фабрика драйверов подтвердила Arduino и ожидаемо отвергла архитектурные metakon/custom_ascii.
Порты при этой проверке не открывались. Проверены 13 локальных Markdown-ссылок.
Все четыре изменённых Steel-сценария прошли steel ast --expanded true --require true;
существующие тесты collections и csv выполнены успешно. Сценарии с изменением мощности не запускались.

git diff --exit-code eee832e подтвердил отсутствие изменений в Cargo.toml/Cargo.lock, драйверах,
модели значений, PortWorker/scheduler, Recorder/SQLite, startup, TCP/WS-адаптерах, DeviceService,
CommandRouter, DeviceRegistry и ObservationAggregator. Комментарии config/protocol обновлены отдельно.
rg и git grep не находят вызовов/экспортов удалённых legacy-символов в src, tests и examples.
Упоминания удалённых имён в этом отчёте — история изменения API, не оставшиеся зависимости.

Созданы: docs/architecture.md, docs/protocol.md, docs/legacy-cleanup-review.md,
src/fanout/test_support.rs, src/protocol/tests.rs, src/session/requests.rs, src/session/subscriptions.rs.
Удалены: src/runtime.rs, src/tcp_legacy.rs. Остальные изменённые файлы не удалялись/переименовывались.
Diff относительно master также включает ранее проверенный eee832e с миграцией pending/tcp/subscriptions/backend;
это единственный исходный коммит предыдущей ветки, а не повторная миграция.

## Повтор критичных сценариев

| Профиль | Повтор 1 | Повтор 2 | Повтор 3 |
| --- | ---: | ---: | ---: |
| Debug | 309 passed | 309 passed | 309 passed |
| Release | 309 passed | 309 passed | 309 passed |

Набор каждого повтора: все 191 unit-тест и 118 интеграционных тестов arduino_designer,
device_registry_router, generic_actions, observation_aggregator, port_worker, startup, tcp_router,
transport_session и websocket. Статические 26 config-тестов входят в полный финальный набор 335.
Итого повторно 1854 успешных исполнения; 0 failed, 0 ignored, 0 предупреждений. Новых sleep в мигрированных стендах нет.
Логи проверок и повторов находятся вне репозитория в D:/rust_projects/lab-server-agent-build/cleanup-*.log.

## Коммиты этапов

- fefdf8cd83b259673f952c53a121e500dbd986ee test: exercise boolean sessions through the modern server
- de1391e218f515058e63d0ebb951124c56a28908 test: migrate connection streams to routed fanout
- 565f003053d76f3c6e531fe6a7b54fb88216d3fa test: use the public device service for transport sessions
- 7a8a2077e76115fb60c3236371cc167a4bdd6312 refactor: remove the single-port runtime and completion adapters
- 38284eb91534e2c48464d021a0ff918c58b2a05f refactor: separate session request tracking and subscription validation

Финальный коммит документации включает этот отчёт; его полный SHA предоставляется в итоговом сообщении.
