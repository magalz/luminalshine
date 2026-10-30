/**
 * @file tests/unit/test_vgd_parent_ring_policy.cpp
 * @brief Unit tests for the parent-side LuminalVGD ring open policy.
 */

#include "src/platform/windows/vgd_parent_ring_policy.h"

#include <chrono>
#include <gtest/gtest.h>

namespace {

  using namespace std::chrono_literals;
  using platf::video_worker::ring_consumers::decision_e;
  using platf::video_worker::ring_consumers::parent_must_skip_ring;
  using platf::video_worker::ring_consumers::registry_t;

  constexpr std::uint64_t ring_a = 0xA001;
  constexpr std::uint64_t ring_b = 0xB002;

  bool live_holder() {
    return false;
  }

  bool retiring_holder() {
    return true;
  }

  TEST(VgdParentRingPolicy, ParentOpensFreeRing) {
    registry_t registry;
    EXPECT_FALSE(parent_must_skip_ring(registry, ring_a, false));
  }

  TEST(VgdParentRingPolicy, ParentSkipsRingHeldByLiveWorker) {
    registry_t registry;
    auto holder = registry.try_acquire(ring_a, live_holder, 0ms);
    ASSERT_EQ(holder.decision, decision_e::claim);

    EXPECT_TRUE(parent_must_skip_ring(registry, ring_a, false));
  }

  TEST(VgdParentRingPolicy, ParentSkipsRingOfRetiringHolder) {
    // The probe never waits for a release; a retiring holder still reads.
    registry_t registry;
    auto holder = registry.try_acquire(ring_a, retiring_holder, 0ms);
    ASSERT_EQ(holder.decision, decision_e::claim);

    EXPECT_TRUE(parent_must_skip_ring(registry, ring_a, false));
  }

  TEST(VgdParentRingPolicy, ParentOpensRingOfAnotherSession) {
    registry_t registry;
    auto holder = registry.try_acquire(ring_a, live_holder, 0ms);
    ASSERT_EQ(holder.decision, decision_e::claim);

    EXPECT_FALSE(parent_must_skip_ring(registry, ring_b, false));
  }

  TEST(VgdParentRingPolicy, ParentOpensRingAfterHolderReleases) {
    registry_t registry;
    auto holder = registry.try_acquire(ring_a, live_holder, 0ms);
    ASSERT_EQ(holder.decision, decision_e::claim);
    holder.lease.reset();

    EXPECT_FALSE(parent_must_skip_ring(registry, ring_a, false));
  }

  TEST(VgdParentRingPolicy, WorkerChildAlwaysOpensItsTransferredRing) {
    // The worker is the ring's admitted consumer; the parent holds its lease.
    registry_t registry;
    auto holder = registry.try_acquire(ring_a, live_holder, 0ms);
    ASSERT_EQ(holder.decision, decision_e::claim);

    EXPECT_FALSE(parent_must_skip_ring(registry, ring_a, true));
  }

}  // namespace
