#ifndef CQ_CIRCULAR_QUEUE_TPP
#define CQ_CIRCULAR_QUEUE_TPP

namespace cq
{

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
CircularQueue<T, Capacity, MaxReaders, Clock>::CircularQueue
(
    std::chrono::milliseconds expiration
)
    : expiration_(expiration)
{
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
void CircularQueue<T, Capacity, MaxReaders, Clock>::write(const T& item) noexcept
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::size_t index = static_cast<std::size_t>(writeSequence_ % Capacity);
        Slot& slot = slots_[index];
        slot.value = item;
        slot.timestamp = Clock::now();
        slot.crc = computeCrc(item);
        slot.sequence = writeSequence_;
        ++writeSequence_;
    }
    itemAdded_.notify_all();
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
std::optional<ReaderId> CircularQueue<T, Capacity, MaxReaders, Clock>::registerReader() noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (std::size_t i = 0U; i < MaxReaders; ++i)
    {
        if (!readers_[i].active)
        {
            readers_[i].active = true;
            // Catch-up from "now": prior items are intentionally invisible.
            readers_[i].nextSequence = writeSequence_;
            return ReaderId{i};
        }
    }
    return std::nullopt;
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
void CircularQueue<T, Capacity, MaxReaders, Clock>::unregisterReader(ReaderId id) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (id.value < MaxReaders)
    {
        readers_[id.value].active = false;
        readers_[id.value].nextSequence = 0U;
    }
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
typename CircularQueue<T, Capacity, MaxReaders, Clock>::Result
CircularQueue<T, Capacity, MaxReaders, Clock>::readLocked(ReaderId id) noexcept
{
    Result result{};

    if (id.value >= MaxReaders || !readers_[id.value].active)
    {
        result.status = ReadStatus::InvalidReader;
        return result;
    }

    ReaderState& reader = readers_[id.value];
    if (reader.nextSequence == writeSequence_)
    {
        result.status = ReadStatus::Empty;
        return result;
    }

    // Oldest sequence still resident in the ring.
    const std::uint64_t oldestAvailable =
        (writeSequence_ > Capacity) ? (writeSequence_ - Capacity) : 0U;

    std::uint64_t lostCount = 0U;
    if (reader.nextSequence < oldestAvailable)
    {
        lostCount = oldestAvailable - reader.nextSequence;
        reader.nextSequence = oldestAvailable;
    }

    const std::size_t index =
        static_cast<std::size_t>(reader.nextSequence % Capacity);
    const Slot& slot = slots_[index];
    result.item = slot.value;
    result.timestamp = slot.timestamp;
    result.lostCount = lostCount;
    ++reader.nextSequence;

    // Classify after advancing so a bad slot cannot stall the reader.
    if (computeCrc(slot.value) != slot.crc)
    {
        result.status = ReadStatus::CrcError;
    }
    else if ((Clock::now() - slot.timestamp) > expiration_)
    {
        result.status = ReadStatus::Expired;
    }
    else if (lostCount > 0U)
    {
        result.status = ReadStatus::Overwritten;
    }
    else
    {
        result.status = ReadStatus::Valid;
    }

    return result;
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
typename CircularQueue<T, Capacity, MaxReaders, Clock>::Result
CircularQueue<T, Capacity, MaxReaders, Clock>::tryRead(ReaderId id) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    return readLocked(id);
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
typename CircularQueue<T, Capacity, MaxReaders, Clock>::Result
CircularQueue<T, Capacity, MaxReaders, Clock>::read
(
    ReaderId id,
    std::chrono::milliseconds timeout
) noexcept
{
    std::unique_lock<std::mutex> lock(mutex_);

    if (id.value >= MaxReaders || !readers_[id.value].active)
    {
        Result result{};
        result.status = ReadStatus::InvalidReader;
        return result;
    }

    const bool ready = itemAdded_.wait_for(lock, timeout, [this, id]() {
        return readers_[id.value].nextSequence < writeSequence_;
    });

    if (!ready)
    {
        Result result{};
        result.status = ReadStatus::Empty;
        return result;
    }

    return readLocked(id);
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
std::size_t CircularQueue<T, Capacity, MaxReaders, Clock>::size() const noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    return (writeSequence_ < Capacity) ? static_cast<std::size_t>(writeSequence_)
                                       : Capacity;
}

template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
bool CircularQueue<T, Capacity, MaxReaders, Clock>::empty() const noexcept
{
    return size() == 0U;
}

#ifdef CQ_ENABLE_TEST_HOOKS
template <typename T, std::size_t Capacity, std::size_t MaxReaders, typename Clock>
void CircularQueue<T, Capacity, MaxReaders, Clock>::corruptCrcForTest
(
    std::uint64_t sequence
) noexcept
{
    std::lock_guard<std::mutex> lock(mutex_);
    const std::size_t index = static_cast<std::size_t>(sequence % Capacity);
    slots_[index].crc ^= 0xFFFFFFFFU;
}
#endif

} // namespace cq

#endif // CQ_CIRCULAR_QUEUE_TPP
