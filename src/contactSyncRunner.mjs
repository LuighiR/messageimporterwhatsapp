export class ContactSyncJobRunner {
  constructor({ client, database, logger = console }) {
    this.client = client;
    this.database = database;
    this.logger = logger;
    this.activeJobs = new Map();
  }

  listJobs(limit = 20) {
    return this.database.listContactSyncJobs(limit);
  }

  getJob(jobId) {
    const job = this.database.getContactSyncJob(jobId);
    if (!job) {
      return null;
    }

    return {
      ...job,
      active: this.activeJobs.has(jobId),
      recentErrors: this.database.listContactSyncJobErrors(jobId, 20)
    };
  }

  startJob({ page = 1, limit = 100, pages = 1, sweepAll = true, maxPages = null }) {
    const runningJob = this.database.getRunningContactSyncJob();
    if (runningJob) {
      throw new Error(`There is already a running contact sync job (${runningJob.jobId}).`);
    }

    const job = this.database.createContactSyncJob({
      page,
      limit,
      pages,
      sweepAll,
      maxPages
    });

    this.runInBackground(job.jobId);
    return this.getJob(job.jobId);
  }

  resumeJob(jobId, { maxPages } = {}) {
    const runningJob = this.database.getRunningContactSyncJob();
    if (runningJob && runningJob.jobId !== jobId) {
      throw new Error(`There is already a running contact sync job (${runningJob.jobId}).`);
    }

    const job = this.database.getContactSyncJob(jobId);
    if (!job) {
      throw new Error(`Contact sync job ${jobId} not found.`);
    }

    if (job.status === "completed") {
      throw new Error(`Contact sync job ${jobId} is already completed.`);
    }

    if (this.activeJobs.has(jobId)) {
      return this.getJob(jobId);
    }

    this.database.updateContactSyncJob(jobId, {
      status: "pending",
      status_message: "Contact sync job queued for resume",
      max_pages: maxPages,
      finished_at: null
    });

    this.runInBackground(jobId);
    return this.getJob(jobId);
  }

  runInBackground(jobId) {
    const promise = this.executeJob(jobId)
      .catch((error) => {
        this.logger?.error?.(`[contact-job] unhandled error in job ${jobId}: ${error.message}`);
      })
      .finally(() => {
        this.activeJobs.delete(jobId);
      });

    this.activeJobs.set(jobId, promise);
  }

  async executeJob(jobId) {
    let job = this.database.getContactSyncJob(jobId);
    if (!job) {
      throw new Error(`Contact sync job ${jobId} not found.`);
    }

    this.logger?.log(`[contact-job] starting job ${jobId} from page ${job.currentPage}`);
    job = this.database.updateContactSyncJob(jobId, {
      status: "running",
      status_message: "Contact sync job is running",
      finished_at: null
    });

    try {
      while (true) {
        this.logger?.log(
          `[contact-job] fetching contacts page ${job.currentPage} with limit ${job.limitPerPage}`
        );
        const payload = await this.client.listContacts({
          page: job.currentPage,
          limit: job.limitPerPage
        });
        const contacts = payload.data || [];
        const totalPages = Number(payload.pagination?.totalPages || 0) || null;

        job = this.database.updateContactSyncJob(jobId, {
          total_pages: totalPages,
          status_message: `Processing contacts page ${job.currentPage}${totalPages ? ` of ${totalPages}` : ""}`
        });

        if (contacts.length === 0) {
          job = this.database.updateContactSyncJob(jobId, {
            status: "completed",
            status_message: "Finished: no more contacts returned by the API.",
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[contact-job] job ${jobId} completed because the API returned no more contacts`);
          return job;
        }

        for (const contact of contacts) {
          try {
            this.database.upsertContact(contact);
            job = this.database.updateContactSyncJob(jobId, {
              contacts_seen: job.contactsSeen + 1,
              contacts_synced: job.contactsSynced + 1,
              status_message: `Synced contact ${contact.id}`
            });
          } catch (error) {
            const message = error instanceof Error ? error.message : "Unknown error";
            this.database.addContactSyncJobError(jobId, {
              contactId: contact.id,
              errorMessage: message
            });
            job = this.database.updateContactSyncJob(jobId, {
              contacts_seen: job.contactsSeen + 1,
              contacts_failed: job.contactsFailed + 1,
              status_message: `Failed contact ${contact.id}`
            });
            this.logger?.error?.(`[contact-job] failed contact ${contact.id} in job ${jobId}: ${message}`);
          }
        }

        const pagesProcessed = job.pagesProcessed + 1;
        const nextPage = job.currentPage + 1;
        job = this.database.updateContactSyncJob(jobId, {
          pages_processed: pagesProcessed,
          current_page: nextPage,
          status_message: `Completed contacts page ${job.currentPage}`
        });

        const reachedRequestedPages = !job.sweepAll && job.pagesRequested && pagesProcessed >= job.pagesRequested;
        const reachedAllPages = job.sweepAll && totalPages && job.currentPage > totalPages;
        const reachedMaxPages = job.maxPages && pagesProcessed >= job.maxPages;

        if (reachedRequestedPages || reachedAllPages) {
          const message = reachedAllPages
            ? "Finished all contact pages."
            : `Finished requested contact pages (${job.pagesRequested}).`;
          job = this.database.updateContactSyncJob(jobId, {
            status: "completed",
            status_message: message,
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[contact-job] job ${jobId} completed`);
          return job;
        }

        if (reachedMaxPages) {
          job = this.database.updateContactSyncJob(jobId, {
            status: "paused",
            status_message: `Paused after reaching maxPages=${job.maxPages}.`,
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[contact-job] job ${jobId} paused after reaching maxPages=${job.maxPages}`);
          return job;
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      job = this.database.updateContactSyncJob(jobId, {
        status: "failed",
        status_message: message,
        finished_at: new Date().toISOString()
      });
      this.logger?.error?.(`[contact-job] job ${jobId} failed: ${message}`);
      return job;
    }
  }
}
