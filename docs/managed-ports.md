# Managed и Standalone PortWorker

Режим фиксируется при создании worker и не имеет setter. Обычный TOML startup
использует только Managed: после подготовки каталогов создаётся одна `Arc<Authority>`
со всеми writable global keys, включая каталоги недоступных портов. Та же Authority
передаётся всем worker и CommandRouter. Драйверы и алгоритмы компонентов не меняются.

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

`PortWorker::new(...)` остаётся совместимым сокращением для Standalone. Этот режим
предназначен для отдельных утилит и тестов драйверов: прямые `PortHandle::set` и
`invoke_action` работают по прежним правилам без создания DeviceService/Authority.
Standalone нельзя превратить в Managed после создания. `CommandRouter::new(registry)`
сохраняет прежнее поведение — создаёт свою Authority; для Managed используйте
`with_authority` с исходной общей Arc. Ошибочно выбранная другая Authority не даёт
доступ к Managed I/O: worker отклонит такой Permit.

В Managed прямой PortHandle-запрос без Permit может быть принят в ограниченную очередь,
но завершится `CommandError::Unauthorized` (`output_not_authorized`) до вызова драйвера.
Это одинаково действует в Manual и Auto, для set и action. Ошибка не останавливает порт,
не считается outcome_unknown и не означает физическую потерю наблюдений. Read/shutdown
сохраняют прежние правила; shutdown не является записью безопасной мощности.

Permit содержит неизменяемые issuer identity и множество ключей. Worker самостоятельно
выводит ожидаемые ключи из своего каталога: один global key для set, все writable-выходы
прибора для action. Проверяется точное совпадение множества и идентичность Authority,
а не только совпадение строк. Permit от другого выхода, другого сервера/Authority,
неполный или избыточный scope не допускается. Для action без writable-выходов всё равно
нужен Permit от правильной Authority с пустым scope. Конкретные имена действий и типов
компонентов не анализируются.

Проверка выполняется непосредственно перед `begin`/`SetExecution::try_start`.
Там сохраняются прежние атомарные проверки deadline, поколений владельца и источников,
source_alive и in-flight. Identity/scope неизменяемы, поэтому между их проверкой и
try_start их нельзя подменить. Отзыв до начала отменяет команду. Начатый I/O удерживает
выход до завершения; таймаут остаётся outcome_unknown, поздний ответ не снимает латч
Auto и не вызывает повтор записи. Физический I/O остаётся только в существующем worker.

Совместимость Rust API: сигнатуры существующих конструкторов, set и invoke_action
сохранены; добавлены PortMode, with_mode и with_authority. В публичный CommandError
добавлен Unauthorized: внешние исчерпывающие match должны обработать новый вариант.
Сетевой формат — v1. Это контроль корректного использования
доверенным Rust-кодом внутри процесса, а не изоляция злонамеренного native-кода.
