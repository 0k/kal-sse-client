export interface ResilientSseOptions {
  url: () => string;
  onMessage: (event: MessageEvent) => void;
  onEvents?: Record<string, (event: MessageEvent) => void>;
  onBeforeReconnect?: () => void | Promise<void>;
  resumeFromLastEventId?: boolean;
  onError?: () => void;
  initialDelayMs?: number;
  maxDelayMs?: number;
  healthyWindowMs?: number;
}

export interface ResilientSseSubscription {
  start(): void;
  stop(): void;
}

export function createResilientSse(opts: ResilientSseOptions): ResilientSseSubscription {
  const initial = opts.initialDelayMs ?? 250;
  const maximum = opts.maxDelayMs ?? 30_000;
  const healthyWindow = opts.healthyWindowMs ?? 5_000;
  let source: EventSource | null = null;
  let delay = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let healthyTimer: ReturnType<typeof setTimeout> | null = null;
  let wake: (() => void) | null = null;
  let running = false;
  let started = false;
  let stopped = false;
  let lastEventId: string | null = null;
  let opened = false;

  function close(): void {
    source?.close();
    source = null;
    if (healthyTimer !== null) clearTimeout(healthyTimer);
    healthyTimer = null;
  }

  function receive(callback: (event: MessageEvent) => void, event: MessageEvent): void {
    if (stopped) return;
    opened = true;
    if (delay > 0 && healthyTimer === null) {
      healthyTimer = setTimeout(() => {
        delay = 0;
        healthyTimer = null;
      }, healthyWindow);
    }
    callback(event);
    if (opts.resumeFromLastEventId && event.lastEventId) lastEventId = event.lastEventId;
  }

  function open(resume = false): void {
    if (stopped) return;
    close();
    opened = false;
    const url = opts.url();
    source = new EventSource(resume ? joinQuery(url, "last_event_id", lastEventId!) : url);
    if (opts.resumeFromLastEventId) source.onopen = () => { opened = true; };
    source.onmessage = (event) => receive(opts.onMessage, event);
    source.onerror = () => {
      if (stopped) return;
      opts.onError?.();
      void reconnect();
    };
    for (const [name, callback] of Object.entries(opts.onEvents ?? {})) {
      source.addEventListener(name, (event) => receive(callback, event as MessageEvent));
    }
  }

  async function reconnect(): Promise<void> {
    if (running || stopped) return;
    running = true;
    const resume = Boolean(opts.resumeFromLastEventId && lastEventId !== null && opened);
    close();
    try {
      if (delay > 0) {
        await new Promise<void>((resolve) => {
          wake = resolve;
          timer = setTimeout(() => {
            timer = null;
            wake = null;
            resolve();
          }, delay);
        });
      }
      if (stopped) return;
      delay = Math.min(delay === 0 ? initial : delay * 2, maximum);
      if (!resume) {
        lastEventId = null;
        await opts.onBeforeReconnect?.();
      }
      if (!stopped) open(resume);
    } catch {
      if (!stopped) {
        timer = setTimeout(() => {
          timer = null;
          running = false;
          void reconnect();
        }, delay);
        return;
      }
    } finally {
      if (timer === null || wake !== null) running = false;
    }
  }

  return {
    start(): void {
      if (started || stopped) return;
      started = true;
      open();
    },
    stop(): void {
      if (stopped) return;
      stopped = true;
      close();
      if (timer !== null) clearTimeout(timer);
      timer = null;
      wake?.();
      wake = null;
    },
  };
}

export interface SnapshotCursorSseOptions<S> {
  snapshotPath: string;
  streamPath: string;
  resolveUrl?: (path: string) => string;
  applySnapshot: (body: S) => void;
  onMessage: (event: MessageEvent) => void;
  retrySnapshotAfter?: (body: S) => number | null;
  onSnapshotError?: (reason: unknown) => void;
  onStreamError?: () => void;
  initialSnapshotRetryMs?: number;
  maxSnapshotRetryMs?: number;
}

export interface SnapshotCursorSseChannel {
  start(): Promise<void>;
  stop(): void;
}

function joinQuery(path: string, key: string, value: string): string {
  return `${path}${path.includes("?") ? "&" : "?"}${key}=${encodeURIComponent(value)}`;
}

export function createSnapshotCursorSse<S>(opts: SnapshotCursorSseOptions<S>): SnapshotCursorSseChannel {
  const resolveUrl = opts.resolveUrl ?? ((path: string) => path);
  let subId: string | null = null;
  let subscription: ResilientSseSubscription | null = null;
  let controller: AbortController | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let wake: (() => void) | null = null;
  let started: Promise<void> | null = null;
  let stopped = false;

  function wait(milliseconds: number): Promise<void> {
    return new Promise<void>((resolve) => {
      wake = resolve;
      timer = setTimeout(() => {
        timer = null;
        wake = null;
        resolve();
      }, milliseconds);
    });
  }

  async function snapshot(onFirstFailure?: () => void): Promise<void> {
    let delay = opts.initialSnapshotRetryMs ?? 250;
    const maxDelay = opts.maxSnapshotRetryMs ?? 30_000;
    let failed = false;
    while (!stopped) {
      const abort = new AbortController();
      controller = abort;
      try {
        const path = subId === null ? opts.snapshotPath : joinQuery(opts.snapshotPath, "sub", subId);
        const response = await fetch(resolveUrl(path), { signal: abort.signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const body = (await response.json()) as S;
        const id = response.headers.get("x-sub-id")?.trim();
        if (!id) throw new Error("Snapshot response missing non-empty x-sub-id");
        const retry = opts.retrySnapshotAfter?.(body) ?? null;
        if (retry !== null) {
          if (!Number.isFinite(retry) || retry < 0) throw new Error("Invalid snapshot retry delay");
          await wait(retry);
          continue;
        }
        if (stopped || abort.signal.aborted) return;
        subId = id;
        opts.applySnapshot(body);
        return;
      } catch (reason) {
        if (stopped || abort.signal.aborted) return;
        opts.onSnapshotError?.(reason);
        if (!failed) onFirstFailure?.();
        failed = true;
        await wait(delay);
        delay = Math.min(delay * 2, maxDelay);
      } finally {
        if (controller === abort) controller = null;
      }
    }
  }

  return {
    start(): Promise<void> {
      if (started !== null) return started;
      if (stopped) return Promise.resolve();
      started = new Promise<void>((resolve) => {
        void (async () => {
          await snapshot(resolve);
          if (!stopped && subId !== null) {
            subscription = createResilientSse({
              url: () => resolveUrl(joinQuery(opts.streamPath, "sub", subId!)),
              onMessage: opts.onMessage,
              resumeFromLastEventId: true,
              ...(opts.onStreamError ? { onError: opts.onStreamError } : {}),
              onBeforeReconnect: () => snapshot(),
            });
            subscription.start();
          }
          resolve();
        })();
      });
      return started;
    },
    stop(): void {
      if (stopped) return;
      stopped = true;
      controller?.abort();
      subscription?.stop();
      if (timer !== null) clearTimeout(timer);
      timer = null;
      wake?.();
      wake = null;
    },
  };
}
