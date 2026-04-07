import { buildTicketImportSnapshot } from "./normalize.mjs";

export class CoreImportJobRunner {
  constructor({ repository, createApiClient, logger = console }) {
    this.repository = repository;
    this.createApiClient = createApiClient;
    this.logger = logger;
    this.activeJobs = new Map();
  }

  async listJobs(limit = 20) {
    return this.repository.listImportJobs(limit);
  }

  async getJob(jobId) {
    const job = await this.repository.getImportJob(jobId);
    if (!job) {
      return null;
    }

    return {
      ...job,
      active: this.activeJobs.has(jobId),
      recent_errors: await this.repository.listImportJobErrors(jobId, 20)
    };
  }

  async startJob({
    clientId,
    page = 1,
    limit = 100,
    pages = 1,
    sweepAll = true,
    maxPages = null,
    persist = true,
    startDate = null,
    endDate = null
  }) {
    const runningJob = await this.repository.getRunningImportJob(clientId);
    if (runningJob) {
      throw new Error(`There is already a running import job for client ${clientId} (${runningJob.job_id}).`);
    }

    const clientConfig = await this.repository.getClient(clientId);
    if (!clientConfig?.is_active) {
      throw new Error(`Client ${clientId} not found or inactive.`);
    }

    const job = await this.repository.createImportJob({
      clientId,
      page,
      limit,
      pages,
      sweepAll,
      maxPages,
      persist,
      startDate,
      endDate
    });

    this.runInBackground(job.job_id);
    return this.getJob(job.job_id);
  }

  async resumeJob(jobId, { maxPages } = {}) {
    const job = await this.repository.getImportJob(jobId);
    if (!job) {
      throw new Error(`Import job ${jobId} not found.`);
    }

    const runningJob = await this.repository.getRunningImportJob(job.client_id);
    if (runningJob && runningJob.job_id !== jobId) {
      throw new Error(`There is already a running import job for client ${job.client_id} (${runningJob.job_id}).`);
    }

    if (job.status === "completed") {
      throw new Error(`Import job ${jobId} is already completed.`);
    }

    if (this.activeJobs.has(jobId)) {
      return this.getJob(jobId);
    }

    await this.repository.updateImportJob(jobId, {
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
        this.logger?.error?.(`[core-import-job] unhandled error in job ${jobId}: ${error.message}`);
      })
      .finally(() => {
        this.activeJobs.delete(jobId);
      });

    this.activeJobs.set(jobId, promise);
  }

  async executeJob(jobId) {
    let job = await this.repository.getImportJob(jobId);
    if (!job) {
      throw new Error(`Import job ${jobId} not found.`);
    }

    const clientConfig = await this.repository.getClient(job.client_id);
    if (!clientConfig?.is_active) {
      throw new Error(`Client ${job.client_id} not found or inactive.`);
    }

    const apiClient = this.createApiClient(clientConfig);
    this.logger?.log(`[core-import-job] starting job ${jobId} for client ${job.client_id} from page ${job.current_page}`);
    job = await this.repository.updateImportJob(jobId, {
      status: "running",
      status_message: "Job is running",
      finished_at: null
    });

    try {
      while (true) {
        this.logger?.log(
          `[core-import-job] fetching tickets page ${job.current_page} with ticketLimit ${job.limit_per_page} for client ${job.client_id}`
        );
        const payload = await apiClient.listTickets({
          page: job.current_page,
          limit: job.limit_per_page,
          startDate: job.start_date || null,
          endDate: job.end_date || null
        });
        const tickets = payload.data || [];
        const totalPages = Number(payload.pagination?.totalPages || 0) || null;

        job = await this.repository.updateImportJob(jobId, {
          total_pages: totalPages,
          status_message: `Processing page ${job.current_page}${totalPages ? ` of ${totalPages}` : ""}`
        });

        if (tickets.length === 0) {
          job = await this.repository.updateImportJob(jobId, {
            status: "completed",
            status_message: "Finished: no more tickets returned by the API.",
            finished_at: new Date().toISOString()
          });
          return job;
        }

        for (let index = job.next_ticket_offset; index < tickets.length; index += 1) {
          const ticket = tickets[index];

          job = await this.repository.updateImportJob(jobId, {
            current_page: job.current_page,
            next_ticket_offset: index,
            current_ticket_uuid: ticket.uuid,
            status_message: `Processing ticket ${ticket.uuid}`
          });

          try {
            const messagesPayload = await apiClient.getAllMessagesByTicketUuid(ticket.uuid, { pageSize: 100 });
            const snapshot = buildTicketImportSnapshot(ticket, messagesPayload.messages);

            if (job.persist === 1) {
              await this.repository.persistTicketSnapshot({
                clientId: job.client_id,
                snapshot
              });
            }

            job = await this.repository.updateImportJob(jobId, {
              next_ticket_offset: index + 1,
              current_ticket_uuid: null,
              tickets_seen: job.tickets_seen + 1,
              tickets_imported: job.tickets_imported + 1,
              status_message: `Imported ticket ${ticket.uuid}`
            });
          } catch (error) {
            const message = error instanceof Error ? error.message : "Unknown error";
            await this.repository.addImportJobError(jobId, {
              ticketId: ticket.id,
              ticketUuid: ticket.uuid,
              errorMessage: message
            });
            job = await this.repository.updateImportJob(jobId, {
              next_ticket_offset: index + 1,
              current_ticket_uuid: null,
              tickets_seen: job.tickets_seen + 1,
              tickets_failed: job.tickets_failed + 1,
              status_message: `Failed ticket ${ticket.uuid}`
            });
            this.logger?.error?.(`[core-import-job] failed ticket ${ticket.uuid}: ${message}`);
          }
        }

        const pagesProcessed = job.pages_processed + 1;
        const nextPage = job.current_page + 1;
        job = await this.repository.updateImportJob(jobId, {
          pages_processed: pagesProcessed,
          current_page: nextPage,
          next_ticket_offset: 0,
          current_ticket_uuid: null,
          status_message: `Completed page ${job.current_page}`
        });

        const reachedRequestedPages =
          job.sweep_all !== 1 && job.pages_requested && pagesProcessed >= job.pages_requested;
        const reachedAllPages = job.sweep_all === 1 && totalPages && job.current_page > totalPages;
        const reachedMaxPages = job.max_pages && pagesProcessed >= job.max_pages;

        if (reachedRequestedPages || reachedAllPages) {
          job = await this.repository.updateImportJob(jobId, {
            status: "completed",
            status_message: reachedAllPages ? "Finished all pages." : `Finished requested pages (${job.pages_requested}).`,
            finished_at: new Date().toISOString()
          });
          return job;
        }

        if (reachedMaxPages) {
          job = await this.repository.updateImportJob(jobId, {
            status: "paused",
            status_message: `Paused after reaching maxPages=${job.max_pages}.`,
            finished_at: new Date().toISOString()
          });
          return job;
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      job = await this.repository.updateImportJob(jobId, {
        status: "failed",
        status_message: message,
        finished_at: new Date().toISOString()
      });
      this.logger?.error?.(`[core-import-job] job ${jobId} failed: ${message}`);
      return job;
    }
  }
}
