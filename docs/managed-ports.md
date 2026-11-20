# Managed и Standalone PortWorker

Режим фиксируется при создании worker и не имеет setter. Обычный TOML startup
использует только Managed: после подготовки каталогов создаётся одна `Arc<Authority>`
со всеми writable global keys, включая каталоги недоступных портов. Та же Authority
передаётся всем worker и CommandRouter.

При самостоятельной композиции Rust-приложения:

```rust,ignore
let authority = Arc::new(Authority::new(writable_global_keys.into_iter()));
let (worker, channels) = PortWorker::with_mode(
    io, devices, command_capacity, observation_capacity,
    PortMode::Managed(authority.clone()),
)?;
// Register channels.commands and worker.devices() in DeviceRegistry.
let router = CommandRouter::with_authority(registry, authority);
```

`PortWorker::new(...)` создаёт Standalone для отдельных утилит и тестов драйверов.
Прямые `PortHandle::set` и `invoke_action` доступны без DeviceService и Authority.
`CommandRouter::new(registry)` создаёт собственную Authority. Для Managed используйте
`with_authority` с общей `Arc<Authority>`. Ошибочно выбранная другая Authority не даёт
доступ к Managed I/O: worker отклонит такой Permit.

В Managed прямой PortHandle-запрос без Permit может быть принят в ограниченную очередь,
но завершится `CommandError::Unauthorized` (`output_not_authorized`) до вызова драйвера.
Это одинаково действует в Manual и Auto, для set и action. Ошибка не останавливает порт,
не считается outcome_unknown и не означает потерю наблюдений. Чтение и shutdown
не требуют Permit. При shutdown команда записи безопасной мощности не отправляется.

Permit содержит неизменяемые issuer identity и множество ключей. Worker самостоятельно
выводит ожидаемые ключи из своего каталога: один global key для set, все writable-выходы
прибора для action. Проверяется точное совпадение множества и идентичность Authority,
а не только совпадение строк. Permit от другого выхода, другого сервера/Authority,
неполный или избыточный scope не допускается. Для action без writable-выходов всё равно
нужен Permit от правильной Authority с пустым scope. Конкретные имена действий и типов
компонентов не анализируются.

Проверка выполняется непосредственно перед `begin`/`SetExecution::try_start`.
Эта граница также атомарно проверяет deadline, поколения владельца и источников,
source_alive и in-flight. Identity/scope неизменяемы, поэтому между их проверкой и
try_start их нельзя подменить. Отзыв до начала отменяет команду. Начатый I/O удерживает
выход до завершения; таймаут остаётся outcome_unknown, поздний ответ не снимает латч
Auto и не вызывает повтор записи. I/O выполняет worker, владеющий портом.

В исчерпывающем `match` по публичному `CommandError` учитывайте `Unauthorized`.
Сетевой формат — v1. Permit предназначен для контроля вызовов доверенного Rust-кода
внутри процесса.
