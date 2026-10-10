import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createSnapshotCursorSse } from "../src/index";

interface FakeSource {
  url: string;
  close: ReturnType<typeof vi.fn>;
  onmessage: ((event: MessageEvent) => void) | null;
  onerror: (() => void) | null;
  onopen: (() => void) | null;
  addEventListener: ReturnType<typeof vi.fn>;
}

const sources: FakeSource[] = [];

function snapshot(body: unknown, subId: string, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name: string) => name.toLowerCase() === "x-sub-id" ? subId : null },
    json: () => Promise.resolve(body),
  } as Response;
}

describe("createSnapshotCursorSse", () => {
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

  it("takes a snapshot before opening a relative stream with only the server subscription id", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(snapshot({ value: 1 }, "sub-one"));
    vi.stubGlobal("fetch", fetchMock);
    const applySnapshot = vi.fn();
    const channel = createSnapshotCursorSse<{ value: number }>({
      snapshotPath: "api/archive",
      streamPath: "api/archive/events",
      resolveUrl: (path) => new URL(path, "https://example.test/serve/rpg/").href,
      applySnapshot,
      onMessage: vi.fn(),
    });

    await channel.start();

    expect(fetchMock).toHaveBeenCalledWith(
      "https://example.test/serve/rpg/api/archive",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(applySnapshot).toHaveBeenCalledWith({ value: 1 });
    expect(sources.map((source) => source.url)).toEqual([
      "https://example.test/serve/rpg/api/archive/events?sub=sub-one",
    ]);
    channel.stop();
  });

  it("re-snapshots before reconnect, keeps the last snapshot through failure, and retries recovery", async () => {
    const applySnapshot = vi.fn();
    const onSnapshotError = vi.fn();
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(snapshot({ value: 1 }, "sub-one"))
      .mockResolvedValueOnce(snapshot({}, "", 503))
      .mockResolvedValueOnce(snapshot({ value: 3 }, "sub-three"));
    vi.stubGlobal("fetch", fetchMock);
    const channel = createSnapshotCursorSse<{ value: number }>({
      snapshotPath: "api/archive",
      streamPath: "api/archive/events",
      resolveUrl: (path) => path,
      applySnapshot,
      onMessage: vi.fn(),
      onSnapshotError,
      initialSnapshotRetryMs: 25,
      maxSnapshotRetryMs: 25,
    });

    await channel.start();
    sources[0]?.onerror?.();
    await Promise.resolve();
    await Promise.resolve();

    expect(applySnapshot.mock.calls).toEqual([[{ value: 1 }]]);
    expect(onSnapshotError).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(25);
    await Promise.resolve();

    expect(applySnapshot.mock.calls).toEqual([[{ value: 1 }], [{ value: 3 }]]);
    expect(fetchMock.mock.calls[1]?.[0]).toBe("api/archive?sub=sub-one");
    expect(sources.map((source) => source.url)).toEqual([
      "api/archive/events?sub=sub-one",
      "api/archive/events?sub=sub-three",
    ]);
    channel.stop();
  });

  function setupResume() {
    const fetchMock = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(snapshot({ value: 1 }, "sub-one"))
      .mockResolvedValue(snapshot({ value: 3 }, "sub-three"));
    vi.stubGlobal("fetch", fetchMock);
    const applySnapshot = vi.fn();
    const channel = createSnapshotCursorSse({
      snapshotPath: "api/archive", streamPath: "api/archive/events",
      applySnapshot, onMessage: vi.fn(),
    });
    return { channel, fetchMock, applySnapshot };
  }

  it("reconnects even when the stream error notification throws", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(snapshot({ value: 1 }, "sub-one"));
    vi.stubGlobal("fetch", fetchMock);
    const onStreamError = vi.fn(() => { throw new Error("notification failed"); });
    const channel = createSnapshotCursorSse({
      snapshotPath: "api/archive", streamPath: "api/archive/events",
      applySnapshot: vi.fn(), onMessage: vi.fn(), onStreamError,
    });
    await channel.start();
    expect(() => sources[0]!.onerror?.()).not.toThrow();
    await vi.advanceTimersByTimeAsync(0);
    expect(onStreamError).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sources).toHaveLength(2);
    channel.stop();
  });

  it("resolves start and retries snapshots even when the snapshot error notification throws", async () => {
    const fetchMock = vi.fn<typeof fetch>()
      .mockRejectedValueOnce(new Error("network unavailable"))
      .mockResolvedValueOnce(snapshot({}, "", 503))
      .mockResolvedValue(snapshot({ value: 3 }, "sub-three"));
    vi.stubGlobal("fetch", fetchMock);
    const onSnapshotError = vi.fn(() => { throw new Error("notification failed"); });
    const applySnapshot = vi.fn();
    const channel = createSnapshotCursorSse({
      snapshotPath: "api/archive", streamPath: "api/archive/events",
      applySnapshot, onMessage: vi.fn(), onSnapshotError,
      initialSnapshotRetryMs: 25, maxSnapshotRetryMs: 50,
    });
    const resolved = vi.fn();
    void channel.start().then(resolved);
    await vi.advanceTimersByTimeAsync(0);
    expect(resolved).toHaveBeenCalledTimes(1);
    expect(sources).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(24);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(49);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(onSnapshotError).toHaveBeenCalledTimes(2);
    expect(applySnapshot.mock.calls).toEqual([[{ value: 3 }]]);
    expect(sources.map((source) => source.url)).toEqual(["api/archive/events?sub=sub-three"]);
    channel.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  async function dropAfterEvents() {
    sources[0]!.onopen?.();
    sources[0]!.onmessage?.(new MessageEvent("message", { lastEventId: "1" }));
    sources[0]!.onmessage?.(new MessageEvent("message", { lastEventId: "2" }));
    sources[0]!.onerror?.();
    await vi.advanceTimersByTimeAsync(0);
  }

  it("resumes applied events with sub and last_event_id without fetching a snapshot", async () => {
    const { channel, fetchMock, applySnapshot } = setupResume();
    await channel.start();
    await dropAfterEvents();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(applySnapshot.mock.calls).toEqual([[{ value: 1 }]]);
    expect(sources[1]!.url).toBe("api/archive/events?sub=sub-one&last_event_id=2");
    channel.stop();
  });

  it("fetches exactly one new snapshot when a resume fails before opening", async () => {
    const { channel, fetchMock, applySnapshot } = setupResume();
    await channel.start();
    await dropAfterEvents();
    sources[1]!.onerror?.();
    await vi.advanceTimersByTimeAsync(250);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1]![0]).toBe("api/archive?sub=sub-one");
    expect(applySnapshot.mock.calls).toEqual([[{ value: 1 }], [{ value: 3 }]]);
    expect(sources[2]!.url).toBe("api/archive/events?sub=sub-three");
    channel.stop();
  });
});
