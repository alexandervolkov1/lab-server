# Исправления review processing-components

Исходные деревья были чистыми, обе рабочие ветки — feature/processing-components.
Начальные HEAD: lab-server `ac1c265ddd3628003be74589d036e85900c073af`,
lab-plot `c53769550d73185030c2bdc814ef65d632cf247d`.

## Исправления

1. **Разрыв истории при rewire.** Engine вычисляет выходы изменённого компонента и
   всех его потомков при успешном изменении inputs. DeviceService передаёт этот набор
   в fanout на общей границе commit с публикацией. Fanout явно закрывает затронутые
   интересы через observation_stream_closed, даже при одинаковом множестве физических
   портов. Старый опубликованный префикс допускается доставить перед терминальным
   событием; значения нового графа туда не попадают. Закрытие необратимо для текущей
   сессии. Независимые подписки и изменение только параметров продолжают работать.
2. **Привязка в снимках.** DeviceService добавляет binding к каждому экземпляру в
   list и ответах всех мутаций. Данные берутся из controls только при совпадении имени
   и incarnation. Null обозначает отсутствие привязки. Binding остаётся видимым в
   Shadow/Stopped/Fault, но не наследуется повторно созданным экземпляром. Метаданные
   добавляются до проверки бюджета страницы 48 KiB; next_offset учитывает усечённую
   страницу. Общая блокировка сервиса обеспечивает соответствие снимка revision.
   Физическая привязка не перенесена в Component или его алгоритмическую конфигурацию.
3. **Managed worker.** TOML startup создаёт общую Authority, передаваемую всем Managed
   worker и роутеру. Непосредственно перед физическим I/O worker проверяет наличие
   Permit, идентичность issuer и точное множество выходов из собственного каталога.
   Затем SetExecution проверяет прежние поколения, сроки, источник и in-flight.
   Прямые PortHandle set/action без Permit отклоняются без передачи байтов, и порт
   остаётся исправным. Отзыв до начала запрещает запись, начатая операция сохраняет
   outcome_unknown и отсутствие автоматического повтора.

Механизмы общие для всех ComponentFactory/Component; алгоритмы EMA/PID, драйверы,
SourceMetadata/Continuity, recorder и схема SQLite v2 не изменены. Их отсутствие
в diff относительно исходного HEAD проверено отдельно. Production-код lab-plot и
scripts/arduino-start.scm также не изменились.

## Коммиты

| Репозиторий | SHA | Содержание |
| --- | --- | --- |
| lab-server | 1c05b587fd321ec1bf7c3dd1297882fccdeb2d5d | Закрытие затронутых историй при rewire |
| lab-server | 8b7f73b8f9834cfb7ddb538584f6f88cb8bd5817 | Binding в снимках, пагинация, TCP/WS проверки |
| lab-server | 11caf19f8e52744329f1f8d4b3eefc268f33b945 | Managed/Standalone, issuer/scope и worker admission |
| lab-plot | 35c6db6e24292636cfe2fd5075135b5e64c5ee94 | Настоящий клиент, Arduino/контроллер через память, примеры |

Этот отчёт и ссылка из отчёта первоначальной реализации фиксируются отдельным
документационным коммитом; его SHA приведён в итоговом сообщении.

Ветки для следующего независимого review:
[lab-server](https://github.com/alexandervolkov1/lab-server/tree/feature/processing-components),
[lab-plot](https://github.com/alexandervolkov1/lab-plot/tree/feature/processing-components).
Master сервера остаётся `78d150ed484aca8932bbc375bf8c6dc59c64f733`, main клиента —
`386459a58d45837b0ba4dae2775936b9194a3e15`. Публикуются только feature-ветки обычным push.
Merge, rebase, force-push, удаление веток, PR, теги и релизы не выполнялись.

## Изменённые файлы

lab-server:

- `src/processing.rs` — набор затронутых выходов и декорирование ограниченной страницы.
- `src/service.rs` — передача разрыва истории и согласованные binding-снимки.
- `src/fanout.rs` — выборочное необратимое закрытие на границе публикации.
- `src/output.rs` — issuer и точное множество ключей Permit.
- `src/pending.rs` — проверка неизменяемого scope разрешения.
- `src/port_worker.rs` — режим при создании и проверка Managed перед I/O.
- `src/command_router.rs` — композиция с общей Authority.
- `src/startup.rs` — Managed для всех обычных серверных портов.
- `src/session/requests.rs` — код ошибки output_not_authorized.
- `src/port_worker/managed_tests.rs` — отказ без байтов, scope/issuer, гонка, timeout.
- `tests/processing_review.rs` — same/cross-port rewire, потомки, независимость,
  retune, конкурентная публикация, binding/incarnation/CAS и размер страниц.
- `tests/websocket.rs` — закрытие старых TCP/WS-сессий, новая подписка,
  чтение binding другим TCP-клиентом и WebSocket-клиентом.
- `docs/processing-api.md`, `docs/processing-architecture.md`, `docs/managed-ports.md`,
  `docs/processing-review.md`, `docs/processing-validation.md` — контракты и отчёты.

lab-plot:

- `tests/server_contract/src/processing.rs` — реальный клиент и реальный Arduino-драйвер
  через имитатор DeviceIo, startup Managed, EMA/PID, binding, Manual/Auto и shutdown.
- `tests/server_contract/Cargo.toml` — актуализация комментария о тестовом запуске.
- `docs/processing.md` — чтение binding из Steel, rewire и результаты проверок.

## Проверки

Использованы существующие CARGO_TARGET_DIR:
`D:\rust_projects\lab-server-agent-build` и `D:\rust_projects\lab-plot-agent-build`.
Журналы этого задания сохранены в них под именами `review-*.log`.

| Набор | Debug | Release |
| --- | --- | --- |
| lab-server, cargo test --all-targets | 387 passed, 0 ignored | 387 passed, 0 ignored |
| lab-plot, cargo test --all-targets | 143 passed, 1 ignored | 143 passed, 1 ignored |
| lab-plot/tests/server_contract | 4 passed | 4 passed |

В обоих репозиториях успешны `cargo fmt --all -- --check`, `cargo check --all-targets`,
`cargo clippy --all-targets -- -D warnings`, `git diff --check`. Отдельный контрактный
crate также прошёл fmt и Clippy. Единственный ignored-тест клиента существовал до
этого задания и требует отдельно запущенного внешнего сервера; его заменяют здесь
отдельные проверки с настоящим сервером и гарантированно имитационным I/O.

Повторные проверки — **по пять запусков в каждом профиле**:

- `cargo test [--release] --lib --test processing_review concurrent_`: в каждом
  запуске 22 библиотечных теста и 1 тест графа. Новые гонки Managed revoke/start и
  rewire/publication выполняют по 32 синхронизированные попытки за запуск.
- Регрессия `loss_before_old_prefix`: x1, x2, потеря x3, x4 с DataLost перед старым
  префиксом, повторные потери, тишина, другой порт, источник команды до обработки
  DataLost сервисом и остановка источника.
- Клиентский `managed_arduino_controller_bindings_and_manual_auto_work_with_real_tcp_and_ws_client`:
  каждый запуск проверяет TCP и WS, прямые запрещённые мутации из TOML-startup,
  Shadow без записи, binding из другой сессии, Auto с Integer output, запрет Manual
  в Auto, ручную запись после Stopped и завершение сервера/worker.

Полные наборы включают прежние проверки SQLite, TCP/WS, outcome_unknown, поздних
ответов, независимости портов, потерь наблюдений и shutdown. Новые проверки не
открывают COM-порты. Ни один тест не подключался к работающему серверу с оборудованием.

## Пример components/list

Контроллер создан и привязан в Shadow; это не разрешение Auto. Полный ответ на
`{"id":7,"op":"components","command":{"action":"list"}}` может выглядеть так:

```json
{
  "id": 7,
  "status": "ok",
  "data": {
    "revision": 2,
    "instances": [{
      "configuration": {
        "name": "loop", "type_id": "pid",
        "inputs": {"input": "furnace1/sample_temperature"},
        "parameters": {
          "setpoint": 23.0, "kp": 1.0, "ki": 0.0, "kd": 0.0,
          "output_min": 0.0, "output_max": 100.0, "max_gap": 10.0
        },
        "max_age_ms": 10000
      },
      "incarnation": 1,
      "state": "shadow",
      "fault": null,
      "binding": {"target": "furnace1/power", "command_timeout_ms": 1000}
    }],
    "total": 1,
    "next_offset": null
  }
}
```

В Steel `(lab/components)` возвращает тот же снимок как association lists;
binding=null превращается в пустой список. Для дополнительных страниц используется
`lab/component!` с next_offset; сравнивайте revision между страницами.

## Совместимость и ограничения

Старые сигнатуры PortWorker::new, PortHandle::set/invoke_action сохранены.
`new` создаёт Standalone; для Managed добавлены PortMode/with_mode и
CommandRouter::with_authority. Внешний исчерпывающий match по публичному CommandError
нужно дополнить вариантом Unauthorized. Низкоуровневый запрос без Permit может
попасть в очередь, но завершится отказом до I/O; ошибку нужно получать через Pending.
Подробный контракт — [managed-ports.md](managed-ports.md).

Scope/issuer защищают границу доверенного Rust API; они не изолируют злонамеренный
native-код процесса. Самостоятельный embeddинг обязан выбрать Managed и одну общую
Authority. Начатую операцию нельзя отменить; outcome_unknown по-прежнему блокирует
Auto до перезапуска. При удалении с незавершённой командой внутренний Control может
удерживаться до её результата; API не показывает эту привязку другому incarnation.

Конфигурации и привязки остаются в RAM. Нет автоматического восстановления COM,
TOML-сохранения компонентов, Emergency, безопасной записи мощности или аппаратных
interlock. Эти механизмы не добавлялись. Реальное оборудование и физическая
устойчивость регулирования не испытывались; запись МЕТАКОН по умолчанию запрещена.
Работа останавливается после публикации feature-веток, до следующего review и слияния.
