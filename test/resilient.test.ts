import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createResilientSse, type ResilientSseOptions } from "../src/index";

interface FakeSource {
  url: string;
  close: ReturnType<typeof vi.fn>;
  onmessage: ((event: MessageEvent) => void) | null;
  onerror: (() => void) | null;
  onopen: (() => void) | null;
  addEventListener: ReturnType<typeof vi.fn>;
}

const sources: FakeSource[] = [];

function setup(options: Partial<ResilientSseOptions> = {}) {
  const onBeforeReconnect = vi.fn();
  const subscription = createResilientSse({
    url: () => "events?sub=one",
    onMessage: vi.fn(),
    onBeforeReconnect,
    resumeFromLastEventId: true,
    initialDelayMs: 100,
    maxDelayMs: 200,
    ...options,
  });
  subscription.start();
  return { subscription, onBeforeReconnect };
}

function message(index: number, lastEventId: string): void {
  sources[index]!.onmessage?.(new MessageEvent("message", { lastEventId }));
}

async function fail(index: number, delay = 0): Promise<void> {
  sources[index]!.onerror?.();
  await vi.advanceTimersByTimeAsync(delay);
}

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
        onopen: null,
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

  it("resumes after the last applied event without running snapshot recovery", async () => {
    const { subscription, onBeforeReconnect } = setup();
    sources[0]!.onopen?.();
    message(0, "1");
    message(0, "2");
    await fail(0);
    expect(sources[1]!.url).toBe("events?sub=one&last_event_id=2");
    expect(onBeforeReconnect).not.toHaveBeenCalled();
    subscription.stop();
  });

  it("retries rejected reconnect hooks with one bounded backoff wait per attempt", async () => {
    const attempts: number[] = [];
    const opens: number[] = [];
    const start = Date.now();
    const { subscription } = setup({
      url: () => { opens.push(Date.now() - start); return "events"; },
      onBeforeReconnect: async () => {
        attempts.push(Date.now() - start);
        if (attempts.length <= 4) throw new Error("recovery unavailable");
      },
    });

    await fail(0);
    expect(attempts).toEqual([0]);
    for (const [wait, time] of [[100, 100], [200, 300], [200, 500], [200, 700]]) {
      await vi.advanceTimersByTimeAsync(wait! - 1);
      expect(attempts[attempts.length - 1]).toBeLessThan(time!);
      await vi.advanceTimersByTimeAsync(1);
      expect(attempts[attempts.length - 1]).toBe(time);
    }
    expect(attempts).toEqual([0, 100, 300, 500, 700]);
    expect(opens).toEqual([0, 700]);
    expect(sources).toHaveLength(2);
    subscription.stop();
  });

  it("cancels rejected reconnect hook retries on stop", async () => {
    const onBeforeReconnect = vi.fn().mockRejectedValue(new Error("recovery unavailable"));
    const { subscription } = setup({ onBeforeReconnect });
    await fail(0);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(1);
    subscription.stop();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(1);
    expect(sources).toHaveLength(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("reconnects even when the error notification throws", async () => {
    const onError = vi.fn(() => { throw new Error("notification failed"); });
    const { subscription, onBeforeReconnect } = setup({ onError });
    expect(() => sources[0]!.onerror?.()).not.toThrow();
    await vi.advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(1);
    expect(sources).toHaveLength(2);
    subscription.stop();
  });

  it("falls back and forgets the id when a resume fails before opening", async () => {
    const { subscription, onBeforeReconnect } = setup();
    message(0, "2");
    await fail(0);
    await fail(1, 100);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(1);
    expect(sources[2]!.url).toBe("events?sub=one");
    sources[2]!.onopen?.();
    await fail(2, 200);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(2);
    expect(sources[3]!.url).toBe("events?sub=one");
    subscription.stop();
  });

  it("runs snapshot recovery when an opened stream has not delivered an id", async () => {
    const { subscription, onBeforeReconnect } = setup();
    sources[0]!.onopen?.();
    await fail(0);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(1);
    expect(sources[1]!.url).toBe("events?sub=one");
    subscription.stop();
  });

  it("keeps snapshot-on-every-reconnect behavior by default", async () => {
    const onBeforeReconnect = vi.fn();
    const subscription = createResilientSse({
      url: () => "events?sub=one", onMessage: vi.fn(), onBeforeReconnect,
    });
    subscription.start();
    sources[0]!.onopen?.();
    message(0, "2");
    await fail(0);
    await fail(1, 250);
    expect(onBeforeReconnect).toHaveBeenCalledTimes(2);
    expect(sources.map((source) => source.url)).toEqual(Array(3).fill("events?sub=one"));
    subscription.stop();
  });

  it("bounds backoff across repeated opened resume failures", async () => {
    const { subscription, onBeforeReconnect } = setup();
    message(0, "2");
    await fail(0);
    for (const [index, delay] of [[1, 100], [2, 200], [3, 200]]) {
      sources[index!]!.onopen?.();
      await fail(index!, delay! - 1);
      expect(sources).toHaveLength(index! + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(sources[index! + 1]!.url).toBe("events?sub=one&last_event_id=2");
    }
    expect(onBeforeReconnect).not.toHaveBeenCalled();
    subscription.stop();
  });

  it("cancels a pending resume on stop", async () => {
    const { subscription, onBeforeReconnect } = setup();
    message(0, "2");
    await fail(0);
    sources[1]!.onopen?.();
    sources[1]!.onerror?.();
    subscription.stop();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sources).toHaveLength(2);
    expect(onBeforeReconnect).not.toHaveBeenCalled();
  });

  it("records only successfully applied ids and preserves them across empty ids", async () => {
    const onMessage = vi.fn().mockImplementationOnce(() => {}).mockImplementationOnce(() => {
      throw new Error("not applied");
    });
    const { subscription } = setup({ onMessage });
    message(0, "1");
    expect(() => message(0, "2")).toThrow("not applied");
    message(0, "");
    await fail(0);
    expect(sources[1]!.url).toBe("events?sub=one&last_event_id=1");
    subscription.stop();
  });

  it("records named events and URL-encodes their ids without an existing query", async () => {
    const callback = vi.fn();
    const { subscription } = setup({ url: () => "events", onEvents: { update: callback } });
    const listener = sources[0]!.addEventListener.mock.calls[0]![1] as (event: MessageEvent) => void;
    listener(new MessageEvent("update", { lastEventId: "a & b" }));
    await fail(0);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(sources[1]!.url).toBe("events?last_event_id=a%20%26%20b");
    subscription.stop();
  });

  it("resets resume backoff after events sustain the healthy window", async () => {
    const { subscription } = setup({ healthyWindowMs: 50 });
    message(0, "1");
    await fail(0);
    message(1, "2");
    await vi.advanceTimersByTimeAsync(50);
    await fail(1);
    expect(sources).toHaveLength(3);
    expect(sources[2]!.url).toBe("events?sub=one&last_event_id=2");
    subscription.stop();
  });
});
