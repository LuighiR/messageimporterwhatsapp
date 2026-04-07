import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { CorzClient } from "../src/corzClient.mjs";
import { CoreImportJobRunner } from "../src/coreImportJobRunner.mjs";
import { ImporterDatabase } from "../src/database.mjs";
import { ImportJobRunner } from "../src/jobRunner.mjs";

class NoopLocalImportRunner extends ImportJobRunner {
  runInBackground() {}
}

class NoopCoreImportRunner extends CoreImportJobRunner {
  runInBackground() {}
}

test("CorzClient.listTickets forwards startDate and endDate to the upstream API", async () => {
  const requests = [];
  const client = new CorzClient({
    baseUrl: "https://example.com/api",
    apiKey: "test-key",
    fetchImpl: async (url) => {
      requests.push(url.toString());
      return {
        ok: true,
        async json() {
          return { data: [], pagination: { totalPages: 0 } };
        }
      };
    },
    logger: null
  });

  await client.listTickets({
    page: 2,
    limit: 100,
    startDate: "2026-04-01",
    endDate: "2026-04-07"
  });

  assert.equal(
    requests[0],
    "https://example.com/api/ticket?page=2&limit=100&startDate=2026-04-01&endDate=2026-04-07"
  );
});

test("ImporterDatabase stores startDate and endDate in import job checkpoints", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dkw-date-range-"));
  const databasePath = path.join(tempDir, "importer.sqlite");
  const database = new ImporterDatabase(databasePath, { logger: null });

  const job = database.createImportJob({
    page: 3,
    limit: 100,
    pages: 5,
    sweepAll: false,
    maxPages: null,
    persist: true,
    startDate: "2026-04-01",
    endDate: "2026-04-07"
  });

  assert.equal(job.startDate, "2026-04-01");
  assert.equal(job.endDate, "2026-04-07");
});

test("Local ImportJobRunner reuses saved date filters when fetching ticket pages", async () => {
  const calls = [];
  const database = {
    getImportJob(jobId) {
      return {
        jobId,
        status: "paused",
        currentPage: 4,
        nextTicketOffset: 0,
        currentTicketUuid: null,
        limitPerPage: 100,
        pagesRequested: null,
        sweepAll: true,
        maxPages: 1,
        persist: true,
        pagesProcessed: 0,
        ticketsSeen: 0,
        ticketsImported: 0,
        ticketsFailed: 0,
        startDate: "2026-04-01",
        endDate: "2026-04-07"
      };
    },
    updateImportJob(jobId, fields) {
      return {
        ...this.getImportJob(jobId),
        ...fields
      };
    },
    addImportJobError() {}
  };
  const client = {
    async listTickets(params) {
      calls.push(params);
      return { data: [], pagination: { totalPages: 0 } };
    }
  };
  const runner = new NoopLocalImportRunner({
    client,
    importService: { importSingleTicket: async () => {} },
    database,
    logger: null
  });

  await runner.executeJob(1);

  assert.deepEqual(calls[0], {
    page: 4,
    limit: 100,
    startDate: "2026-04-01",
    endDate: "2026-04-07"
  });
});

test("CoreImportJobRunner stores date filters when a tenant job is created", async () => {
  let capturedCreateArgs = null;
  const repository = {
    async getRunningImportJob() {
      return null;
    },
    async getClient(clientId) {
      return { id: clientId, is_active: true };
    },
    async createImportJob(args) {
      capturedCreateArgs = args;
      return {
        job_id: 17,
        client_id: args.clientId,
        status: "pending",
        start_date: args.startDate,
        end_date: args.endDate
      };
    },
    async getImportJob() {
      return {
        job_id: 17,
        client_id: "tenant-a",
        status: "pending",
        start_date: "2026-04-01",
        end_date: "2026-04-07"
      };
    },
    async listImportJobErrors() {
      return [];
    }
  };
  const runner = new NoopCoreImportRunner({
    repository,
    createApiClient: () => ({}),
    logger: null
  });

  await runner.startJob({
    clientId: "tenant-a",
    startDate: "2026-04-01",
    endDate: "2026-04-07"
  });

  assert.equal(capturedCreateArgs.startDate, "2026-04-01");
  assert.equal(capturedCreateArgs.endDate, "2026-04-07");
});

test("CoreImportJobRunner reuses saved date filters when resuming ticket fetches", async () => {
  const calls = [];
  const repository = {
    async getImportJob(jobId) {
      return {
        job_id: jobId,
        client_id: "tenant-a",
        status: "paused",
        current_page: 6,
        next_ticket_offset: 0,
        current_ticket_uuid: null,
        limit_per_page: 100,
        pages_requested: null,
        sweep_all: 1,
        max_pages: 1,
        persist: 1,
        pages_processed: 0,
        tickets_seen: 0,
        tickets_imported: 0,
        tickets_failed: 0,
        start_date: "2026-04-01",
        end_date: "2026-04-07"
      };
    },
    async getClient(clientId) {
      return { id: clientId, is_active: true };
    },
    async updateImportJob(jobId, fields) {
      return {
        ...(await this.getImportJob(jobId)),
        ...fields
      };
    },
    async addImportJobError() {}
  };
  const runner = new NoopCoreImportRunner({
    repository,
    createApiClient: () => ({
      async listTickets(params) {
        calls.push(params);
        return { data: [], pagination: { totalPages: 0 } };
      }
    }),
    logger: null
  });

  await runner.executeJob(22);

  assert.deepEqual(calls[0], {
    page: 6,
    limit: 100,
    startDate: "2026-04-01",
    endDate: "2026-04-07"
  });
});
