#include "cq/circular_queue.hpp"
#include "test_clock.hpp"
#include "test_types.hpp"

#include <gtest/gtest.h>

#include <thread>

using QueueInt = cq::CircularQueue<int, 4, 4, TestClock>;
using QueueData = cq::CircularQueue<Data, 4, 2, TestClock>;
using QueueCap1 = cq::CircularQueue<int, 1, 2, TestClock>;

class QueueTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        TestClock::reset();
    }
};

TEST_F(QueueTest, WriteReadValid)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.write(7);
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::Valid);
    EXPECT_EQ(r.item, 7);
    EXPECT_EQ(r.lostCount, 0U);
}

TEST_F(QueueTest, EmptyQueue)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.tryRead(*id).status, cq::ReadStatus::Empty);
}

TEST_F(QueueTest, WrapAround)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    for (int i = 0; i < 4; ++i)
    {
        q.write(i);
    }
    for (int i = 0; i < 4; ++i)
    {
        auto r = q.tryRead(*id);
        EXPECT_EQ(r.status, cq::ReadStatus::Valid);
        EXPECT_EQ(r.item, i);
    }
    EXPECT_EQ(q.tryRead(*id).status, cq::ReadStatus::Empty);
}

TEST_F(QueueTest, OverwriteReportsLostAndDelivers)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    for (int i = 0; i < 6; ++i)
    {
        q.write(i);
    }
    // Sequences 0,1 overwritten; oldest available is 2; deliver 2 with lost=2.
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::Overwritten);
    EXPECT_EQ(r.lostCount, 2U);
    EXPECT_EQ(r.item, 2);
}

TEST_F(QueueTest, IndependentReaders)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto a = q.registerReader();
    auto b = q.registerReader();
    ASSERT_TRUE(a && b);
    q.write(1);
    q.write(2);
    EXPECT_EQ(q.tryRead(*a).item, 1);
    EXPECT_EQ(q.tryRead(*b).item, 1);
    EXPECT_EQ(q.tryRead(*a).item, 2);
}

TEST_F(QueueTest, StructCrcValid)
{
    QueueData q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.write(Data{9U, 8U, 7U});
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::Valid);
    EXPECT_EQ(r.item.id, 9U);
}

TEST_F(QueueTest, CrcError)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.write(5);
    q.corruptCrcForTest(0U);
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::CrcError);
    EXPECT_EQ(r.item, 5);
    EXPECT_EQ(q.tryRead(*id).status, cq::ReadStatus::Empty);
}

TEST_F(QueueTest, TimestampAndExpiration)
{
    QueueInt q(std::chrono::milliseconds{100});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.write(1);
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::Valid);
    EXPECT_EQ(r.timestamp, TestClock::now());

    q.write(2);
    TestClock::advance(std::chrono::milliseconds{101});
    auto e = q.tryRead(*id);
    EXPECT_EQ(e.status, cq::ReadStatus::Expired);
    EXPECT_EQ(e.item, 2);
}

TEST_F(QueueTest, CapacityOne)
{
    QueueCap1 q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.write(1);
    q.write(2);
    auto r = q.tryRead(*id);
    EXPECT_EQ(r.status, cq::ReadStatus::Overwritten);
    EXPECT_EQ(r.lostCount, 1U);
    EXPECT_EQ(r.item, 2);
}

TEST_F(QueueTest, MaxReadersExhausted)
{
    cq::CircularQueue<int, 2, 1, TestClock> q(std::chrono::milliseconds{1000});
    ASSERT_TRUE(q.registerReader().has_value());
    EXPECT_FALSE(q.registerReader().has_value());
}

TEST_F(QueueTest, InvalidAndUnregister)
{
    QueueInt q(std::chrono::milliseconds{1000});
    EXPECT_EQ(q.tryRead(cq::ReaderId{99}).status, cq::ReadStatus::InvalidReader);
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    q.unregisterReader(*id);
    EXPECT_EQ(q.tryRead(*id).status, cq::ReadStatus::InvalidReader);
    auto again = q.registerReader();
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->value, id->value);
}

TEST_F(QueueTest, BlockingReadTimeoutAndNotify)
{
    QueueInt q(std::chrono::milliseconds{1000});
    auto id = q.registerReader();
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(q.read(*id, std::chrono::milliseconds{10}).status, cq::ReadStatus::Empty);

    std::thread writer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        q.write(99);
    });
    auto r = q.read(*id, std::chrono::milliseconds{500});
    writer.join();
    EXPECT_EQ(r.status, cq::ReadStatus::Valid);
    EXPECT_EQ(r.item, 99);
}

TEST_F(QueueTest, SizeWhenFull)
{
    QueueInt q(std::chrono::milliseconds{1000});
    EXPECT_EQ(q.capacity(), 4U);
    for (int i = 0; i < 4; ++i)
    {
        q.write(i);
    }
    EXPECT_EQ(q.size(), 4U);
    q.write(4);
    EXPECT_EQ(q.size(), 4U);
}
