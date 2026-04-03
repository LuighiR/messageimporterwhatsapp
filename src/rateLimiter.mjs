function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export class SlidingWindowRateLimiter {
  constructor({ maxRequests, windowMs }) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
    this.timestamps = [];
    this.queue = Promise.resolve();
  }

  async acquire() {
    const previous = this.queue;
    let releaseCurrent;
    this.queue = new Promise((resolve) => {
      releaseCurrent = resolve;
    });

    await previous;

    try {
      while (true) {
        const now = Date.now();
        this.timestamps = this.timestamps.filter((timestamp) => now - timestamp < this.windowMs);

        if (this.timestamps.length < this.maxRequests) {
          this.timestamps.push(now);
          return;
        }

        const oldestTimestamp = this.timestamps[0];
        const waitMs = Math.max(this.windowMs - (now - oldestTimestamp), 50);
        await sleep(waitMs);
      }
    } finally {
      releaseCurrent();
    }
  }
}
