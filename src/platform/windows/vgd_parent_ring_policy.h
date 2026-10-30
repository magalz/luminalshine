/**
 * @file src/platform/windows/vgd_parent_ring_policy.h
 * @brief Whether the parent process may open a LuminalVGD frame ring itself.
 *
 * The parent opens rings outside the worker path too: the encoder probe runs
 * on /launch, /resume, WebRTC start and startup, and an in-process pipeline
 * captures directly. While a live worker consumes a ring, any such open is a
 * second consumer of a single-consumer ring (and may trigger a planned modeset
 * on that session's display), so the parent falls back to WGC/DDA instead.
 *
 * Standard library only, so it compiles and tests without the Windows runtime.
 */
#pragma once

#include "src/platform/windows/vgd_ring_consumers.h"

#include <cstdint>

namespace platf::video_worker::ring_consumers {

  /**
   * @param registry The parent's ring consumer registry.
   * @param session_id Ring session id of the display about to be opened.
   * @param worker_child True in an isolated worker opening the ring the parent
   *        transferred to it. The worker is the admitted consumer.
   * @return True when the caller must not open the ring. A retiring holder
   *         still counts: the caller does not wait for its release.
   */
  inline bool parent_must_skip_ring(const registry_t &registry, std::uint64_t session_id, bool worker_child) {
    return !worker_child && registry.is_held(session_id);
  }

}  // namespace platf::video_worker::ring_consumers
