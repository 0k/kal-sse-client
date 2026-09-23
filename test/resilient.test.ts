import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createResilientSse } from "../src/index";

interface FakeSource {
  url: string;
  close: ReturnType<typeof vi.fn>;
  onmessage: ((event: MessageEvent) => void) | null;
  onerror: (() => void) | null;
  addEventListener: ReturnType<typeof vi.fn>;
}

const sources: FakeSource[] = [];

describe("createResilientSse", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    sources.length = 0;
    vi.stubGlobal("EventSource", vi.fn().mockImplementation((url: string) => {
      const source: FakeSource = {
        url,
        close: vi.fn(),
        onmessage: null,
        onerror: null,
        addEventListener: vi.fn(),
      };
      sources.push(source);
      return source;
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("uses bounded exponential backoff and cancels pending reconnect on stop", async () => {
    const subscription = createResilientSse({
      url: () => "events?sub=one",
      onMessage: vi.fn(),
      initialDelayMs: 100,
      maxDelayMs: 200,
    });

    subscription.start();
    sources[0]?.onerror?.();
    await Promise.resolve();
    expect(sources).toHaveLength(2);

    sources[1]?.onerror?.();
    await vi.advanceTimersByTimeAsync(99);
    expect(sources).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(sources).toHaveLength(3);

    sources[2]?.onerror?.();
    subscription.stop();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sources).toHaveLength(3);
  });
});
