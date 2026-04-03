export class CoreContactSyncJobRunner {
  constructor({ repository, createApiClient, logger = console }) {
    this.repository = repository;
    this.createApiClient = createApiClient;
    this.logger = logger;
    this.activeJobs = new Map();
  }

  async listJobs(limit = 20) {
    return this.repository.listContactSyncJobs(limit);
  }

  async getJob(jobId) {
    const job = await this.repository.getContactSyncJob(jobId);
    if (!job) {
      return null;
    }

    return {
      ...job,
      active: this.activeJobs.has(jobId),
      recent_errors: await this.repository.listContactSyncJobErrors(jobId, 20)
    };
  }

  async startJob({ clientId, page = 1, limit = 100, pages = 1, sweepAll = true, maxPages = null }) {
    const runningJob = await this.repository.getRunningContactSyncJob(clientId);
    if (runningJob) {
      throw new Error(`There is already a running contact sync job for client ${clientId} (${runningJob.job_id}).`);
    }

    const clientConfig = await this.repository.getClient(clientId);
    if (!clientConfig?.is_active) {
      throw new Error(`Client ${clientId} not found or inactive.`);
    }

    const job = await this.repository.createContactSyncJob({
      clientId,
      page,
      limit,
      pages,
      sweepAll,
      maxPages
    });

    this.runInBackground(job.job_id);
    return this.getJob(job.job_id);
  }

  async resumeJob(jobId, { maxPages } = {}) {
    const job = await this.repository.getContactSyncJob(jobId);
    if (!job) {
      throw new Error(`Contact sync job ${jobId} not found.`);
    }

    const runningJob = await this.repository.getRunningContactSyncJob(job.client_id);
    if (runningJob && runningJob.job_id !== jobId) {
      throw new Error(
        `There is already a running contact sync job for client ${job.client_id} (${runningJob.job_id}).`
      );
    }

    if (job.status === "completed") {
      throw new Error(`Contact sync job ${jobId} is already completed.`);
    }

    if (this.activeJobs.has(jobId)) {
      return this.getJob(jobId);
    }

    await this.repository.updateContactSyncJob(jobId, {
      status: "pending",
      status_message: "Job queued for resume",
      max_pages: maxPages,
      finished_at: null
    });

    this.runInBackground(jobId);
    return this.getJob(jobId);
  }

  runInBackground(jobId) {
    const promise = this.executeJob(jobId)
      .catch((error) => {
        this.logger?.error?.(`[core-contact-job] unhandled error in job ${jobId}: ${error.message}`);
      })
      .finally(() => {
        this.activeJobs.delete(jobId);
      });

    this.activeJobs.set(jobId, promise);
  }

  async executeJob(jobId) {
    let job = await this.repository.getContactSyncJob(jobId);
    if (!job) {
      throw new Error(`Contact sync job ${jobId} not found.`);
    }

    const clientConfig = await this.repository.getClient(job.client_id);
    if (!clientConfig?.is_active) {
      throw new Error(`Client ${job.client_id} not found or inactive.`);
    }

    const apiClient = this.createApiClient(clientConfig);
    this.logger?.log(`[core-contact-job] starting job ${jobId} for client ${job.client_id} from page ${job.current_page}`);
    job = await this.repository.updateContactSyncJob(jobId, {
      status: "running",
      status_message: "Contact sync job is running",
      finished_at: null
    });

    try {
      while (true) {
        const payload = await apiClient.listContacts({
          page: job.current_page,
          limit: job.limit_per_page
        });
        const contacts = payload.data || [];
        const totalPages = Number(payload.pagination?.totalPages || 0) || null;

        job = await this.repository.updateContactSyncJob(jobId, {
          total_pages: totalPages,
          status_message: `Processing contacts page ${job.current_page}${totalPages ? ` of ${totalPages}` : ""}`
        });

        if (contacts.length === 0) {
          job = await this.repository.updateContactSyncJob(jobId, {
            status: "completed",
            status_message: "Finished: no more contacts returned by the API.",
            finished_at: new Date().toISOString()
          });
          return job;
        }

        for (const contact of contacts) {
          try {
            await this.repository.upsertContact(job.client_id, contact);
            job = await this.repository.updateContactSyncJob(jobId, {
              contacts_seen: job.contacts_seen + 1,
              contacts_synced: job.contacts_synced + 1,
              status_message: `Synced contact ${contact.id}`
            });
          } catch (error) {
            const message = error instanceof Error ? error.message : "Unknown error";
            await this.repository.addContactSyncJobError(jobId, {
              contactId: contact.id,
              errorMessage: message
            });
            job = await this.repository.updateContactSyncJob(jobId, {
              contacts_seen: job.contacts_seen + 1,
              contacts_failed: job.contacts_failed + 1,
              status_message: `Failed contact ${contact.id}`
            });
            this.logger?.error?.(`[core-contact-job] failed contact ${contact.id}: ${message}`);
          }
        }

        const pagesProcessed = job.pages_processed + 1;
        const nextPage = job.current_page + 1;
        job = await this.repository.updateContactSyncJob(jobId, {
          pages_processed: pagesProcessed,
          current_page: nextPage,
          status_message: `Completed contacts page ${job.current_page}`
        });

        const reachedRequestedPages =
          job.sweep_all !== 1 && job.pages_requested && pagesProcessed >= job.pages_requested;
        const reachedAllPages = job.sweep_all === 1 && totalPages && job.current_page > totalPages;
        const reachedMaxPages = job.max_pages && pagesProcessed >= job.max_pages;

        if (reachedRequestedPages || reachedAllPages) {
          job = await this.repository.updateContactSyncJob(jobId, {
            status: "completed",
            status_message: reachedAllPages
              ? "Finished all contact pages."
              : `Finished requested contact pages (${job.pages_requested}).`,
            finished_at: new Date().toISOString()
          });
          return job;
        }

        if (reachedMaxPages) {
          job = await this.repository.updateContactSyncJob(jobId, {
            status: "paused",
            status_message: `Paused after reaching maxPages=${job.max_pages}.`,
            finished_at: new Date().toISOString()
          });
          return job;
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      job = await this.repository.updateContactSyncJob(jobId, {
        status: "failed",
        status_message: message,
        finished_at: new Date().toISOString()
      });
      this.logger?.error?.(`[core-contact-job] job ${jobId} failed: ${message}`);
      return job;
    }
  }
}
