#include "cq/circular_queue.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

TEST(Concurrency, MultiWriter)
{
    cq::CircularQueue<int, 256, 4> q(std::chrono::milliseconds{60000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());

    constexpr int kPerWriter = 100;
    constexpr int kWriters = 4;
    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w)
    {
        writers.emplace_back([&q, w]() {
            for (int i = 0; i < kPerWriter; ++i)
            {
                q.write(w * kPerWriter + i);
            }
        });
    }
    for (std::thread& t : writers)
    {
        t.join();
    }

    std::set<int> seen;
    for (;;)
    {
        auto r = q.tryRead(*id);
        if (r.status == cq::ReadStatus::Empty)
        {
            break;
        }
        ASSERT_TRUE(r.status == cq::ReadStatus::Valid ||
                    r.status == cq::ReadStatus::Overwritten);
        seen.insert(r.item);
    }
    // Capacity 256, total writes 400 → 144 overwritten before first read.
    EXPECT_EQ(seen.size(), 256U);
}

TEST(Concurrency, MultiReaderIndependence)
{
    cq::CircularQueue<int, 64, 4> q(std::chrono::milliseconds{60000});
    auto a = q.registerReader();
    auto b = q.registerReader();
    ASSERT_TRUE(a && b);

    for (int i = 0; i < 32; ++i)
    {
        q.write(i);
    }

    std::vector<int> fromA;
    std::vector<int> fromB;
    for (int i = 0; i < 32; ++i)
    {
        fromA.push_back(q.tryRead(*a).item);
        fromB.push_back(q.tryRead(*b).item);
    }
    EXPECT_EQ(fromA, fromB);
}

TEST(Concurrency, SimultaneousReaderWriter)
{
    cq::CircularQueue<int, 128, 2> q(std::chrono::milliseconds{60000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());

    constexpr int kN = 1000;
    std::atomic<bool> done{false};
    std::atomic<int> accounted{0};
    std::thread writer([&]() {
        for (int i = 0; i < kN; ++i)
        {
            q.write(i);
        }
        done.store(true);
    });
    std::thread reader([&]() {
        int last = -1;
        while (!done.load() || accounted.load() < kN)
        {
            auto r = q.read(*id, std::chrono::milliseconds{20});
            if (r.status == cq::ReadStatus::Empty)
            {
                if (done.load() && accounted.load() >= kN)
                {
                    break;
                }
                continue;
            }
            if (r.status == cq::ReadStatus::Valid ||
                r.status == cq::ReadStatus::Overwritten)
            {
                EXPECT_GT(r.item, last);
                last = r.item;
            }
            accounted.fetch_add(1 + static_cast<int>(r.lostCount));
        }
    });
    writer.join();
    reader.join();
    EXPECT_EQ(accounted.load(), kN);
}
