# Circular Queue — Design Note

## Architecture

Fixed ring of `Capacity` slots in `std::array`. A monotonic `writeSequence_`
counts every write; slot index is `writeSequence_ % Capacity`. Readers hold
an independent `nextSequence` cursor in a fixed `MaxReaders` table.

## Multi-writer / multi-reader

One `std::mutex` serializes all access. Writers never block on full — they
overwrite. Readers never remove items; reading only advances that reader's
cursor. Chosen over lock-free: bounded critical section, simple reasoning,
no ABA, MISRA-friendlier than atomics+fences.

## Overwrite / lost items

`oldestAvailable = writeSequence_ > Capacity ? writeSequence_ - Capacity : 0`.
If `nextSequence < oldestAvailable`, `lostCount` is the gap, the cursor snaps
forward, and that call still delivers the oldest surviving item. Status is
`Overwritten` when the delivered slot is otherwise valid; `CrcError` /
`Expired` take precedence if the delivered slot is bad.

## Notification

`std::condition_variable` with `notify_all` after each write. `read(timeout)`
waits on `nextSequence < writeSequence_` under the same mutex — no lost wakeups.
`tryRead` never waits.

## CRC

Trait-based `CrcTraits<T>` feeds defined members into CRC-32/IEEE. No
`reinterpret_cast`, no padding bytes. Integral/enum types get a default;
structs specialize once.

## Timestamp / expiration

`Clock` is a template policy (default `steady_clock`). Write stamps
`Clock::now()`. Read reports `Expired` when `now - timestamp > expiration`.
Tests inject `TestClock`.

## New-reader policy

`registerReader` starts at the current `writeSequence_` — only future items.

## Limitations

- Slow readers lose data by design (overwrite).
- One CV wakes all waiters (acceptable at small MaxReaders).
- `uint64_t` sequence assumed not to wrap in product lifetime.

## MISRA C++:2023 deviations

| Area | Why |
|------|-----|
| `<mutex>`, `<condition_variable>`, `<chrono>`, `<optional>` | Required for the chosen portable sync/time model |
| Class templates / `.tpp` | Capacity and type must be compile-time |
| `CQ_ENABLE_TEST_HOOKS` CRC corruptor | Test-only; not in production builds |
