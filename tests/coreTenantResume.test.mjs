import assert from "node:assert/strict";
import test from "node:test";

import { CoreContactSyncJobRunner } from "../src/coreContactSyncJobRunner.mjs";
import { CoreImportJobRunner } from "../src/coreImportJobRunner.mjs";

class NoopImportRunner extends CoreImportJobRunner {
  runInBackground() {}
}

class NoopContactRunner extends CoreContactSyncJobRunner {
  runInBackground() {}
}

function createImportRepositoryDouble() {
  const jobs = new Map();

  return {
    async getRunningImportJob(clientId) {
      if (clientId === "tenant-b") {
        return null;
      }

      return {
        job_id: 99,
        client_id: "tenant-a",
        status: "running"
      };
    },
    async getClient(clientId) {
      return {
        id: clientId,
        is_active: true
      };
    },
    async createImportJob({ clientId, page, limit, pages, sweepAll, maxPages, persist }) {
      const job = {
        job_id: 7,
        client_id: clientId,
        status: "pending",
        current_page: page,
        next_ticket_offset: 0,
        current_ticket_uuid: null,
        limit_per_page: limit,
        pages_requested: pages,
        sweep_all: sweepAll ? 1 : 0,
        max_pages: maxPages,
        persist: persist ? 1 : 0
      };
      jobs.set(job.job_id, job);
      return job;
    },
    async getImportJob(jobId) {
      return jobs.get(jobId) || {
        job_id: jobId,
        client_id: "tenant-b",
        status: "paused",
        current_page: 3,
        next_ticket_offset: 0,
        current_ticket_uuid: null,
        limit_per_page: 100,
        pages_requested: null,
        sweep_all: 1,
        max_pages: null,
        persist: 1
      };
    },
    async updateImportJob(jobId, fields) {
      const current = await this.getImportJob(jobId);
      const next = { ...current, ...fields };
      jobs.set(jobId, next);
      return next;
    },
    async listImportJobErrors() {
      return [];
    }
  };
}

function createContactRepositoryDouble() {
  const jobs = new Map();

  return {
    async getRunningContactSyncJob(clientId) {
      if (clientId === "tenant-b") {
        return null;
      }

      return {
        job_id: 88,
        client_id: "tenant-a",
        status: "running"
      };
    },
    async getClient(clientId) {
      return {
        id: clientId,
        is_active: true
      };
    },
    async createContactSyncJob({ clientId, page, limit, pages, sweepAll, maxPages }) {
      const job = {
        job_id: 11,
        client_id: clientId,
        status: "pending",
        current_page: page,
        limit_per_page: limit,
        pages_requested: pages,
        sweep_all: sweepAll ? 1 : 0,
        max_pages: maxPages
      };
      jobs.set(job.job_id, job);
      return job;
    },
    async getContactSyncJob(jobId) {
      return jobs.get(jobId) || {
        job_id: jobId,
        client_id: "tenant-b",
        status: "paused",
        current_page: 4,
        limit_per_page: 100,
        pages_requested: null,
        sweep_all: 1,
        max_pages: null
      };
    },
    async updateContactSyncJob(jobId, fields) {
      const current = await this.getContactSyncJob(jobId);
      const next = { ...current, ...fields };
      jobs.set(jobId, next);
      return next;
    },
    async listContactSyncJobErrors() {
      return [];
    }
  };
}

test("Core import startJob allows another tenant to keep running", async () => {
  const repository = createImportRepositoryDouble();
  const runner = new NoopImportRunner({
    repository,
    createApiClient: () => ({})
  });

  const job = await runner.startJob({
    clientId: "tenant-b"
  });

  assert.equal(job.client_id, "tenant-b");
  assert.equal(job.job_id, 7);
});

test("Core import resumeJob scopes running-job check to the paused job tenant", async () => {
  const repository = createImportRepositoryDouble();
  const runner = new NoopImportRunner({
    repository,
    createApiClient: () => ({})
  });

  const job = await runner.resumeJob(7);

  assert.equal(job.client_id, "tenant-b");
  assert.equal(job.status, "pending");
});

test("Core contact startJob allows another tenant to keep running", async () => {
  const repository = createContactRepositoryDouble();
  const runner = new NoopContactRunner({
    repository,
    createApiClient: () => ({})
  });

  const job = await runner.startJob({
    clientId: "tenant-b"
  });

  assert.equal(job.client_id, "tenant-b");
  assert.equal(job.job_id, 11);
});

test("Core contact resumeJob scopes running-job check to the paused job tenant", async () => {
  const repository = createContactRepositoryDouble();
  const runner = new NoopContactRunner({
    repository,
    createApiClient: () => ({})
  });

  const job = await runner.resumeJob(11);

  assert.equal(job.client_id, "tenant-b");
  assert.equal(job.status, "pending");
});
