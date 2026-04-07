export class ImportJobRunner {
  constructor({ client, importService, database, logger = console }) {
    this.client = client;
    this.importService = importService;
    this.database = database;
    this.logger = logger;
    this.activeJobs = new Map();
  }

  listJobs(limit = 20) {
    return this.database.listImportJobs(limit);
  }

  getJob(jobId) {
    const job = this.database.getImportJob(jobId);
    if (!job) {
      return null;
    }

    return {
      ...job,
      active: this.activeJobs.has(jobId),
      recentErrors: this.database.listImportJobErrors(jobId, 20)
    };
  }

  startJob({
    page = 1,
    limit = 100,
    pages = 1,
    sweepAll = false,
    maxPages = null,
    persist = true,
    startDate = null,
    endDate = null
  }) {
    const runningJob = this.database.getRunningImportJob();
    if (runningJob) {
      throw new Error(`There is already a running job (${runningJob.jobId}).`);
    }

    const job = this.database.createImportJob({
      page,
      limit,
      pages,
      sweepAll,
      maxPages,
      persist,
      startDate,
      endDate
    });

    this.runInBackground(job.jobId);
    return this.getJob(job.jobId);
  }

  resumeJob(jobId, { maxPages } = {}) {
    const runningJob = this.database.getRunningImportJob();
    if (runningJob && runningJob.jobId !== jobId) {
      throw new Error(`There is already a running job (${runningJob.jobId}).`);
    }

    const job = this.database.getImportJob(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found.`);
    }

    if (job.status === "completed") {
      throw new Error(`Job ${jobId} is already completed.`);
    }

    if (this.activeJobs.has(jobId)) {
      return this.getJob(jobId);
    }

    this.database.updateImportJob(jobId, {
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
        this.logger?.error?.(`[job] unhandled error in job ${jobId}: ${error.message}`);
      })
      .finally(() => {
        this.activeJobs.delete(jobId);
      });

    this.activeJobs.set(jobId, promise);
  }

  async executeJob(jobId) {
    let job = this.database.getImportJob(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found.`);
    }

    this.logger?.log(`[job] starting job ${jobId} from page ${job.currentPage}`);
    job = this.database.updateImportJob(jobId, {
      status: "running",
      status_message: "Job is running",
      finished_at: null
    });

    try {
      while (true) {
        this.logger?.log(`[job] fetching ticket page ${job.currentPage} with limit ${job.limitPerPage}`);
        const payload = await this.client.listTickets({
          page: job.currentPage,
          limit: job.limitPerPage,
          startDate: job.startDate,
          endDate: job.endDate
        });
        const tickets = payload.data || [];
        const totalPages = Number(payload.pagination?.totalPages || 0) || null;

        job = this.database.updateImportJob(jobId, {
          total_pages: totalPages,
          status_message: `Processing page ${job.currentPage}${totalPages ? ` of ${totalPages}` : ""}`
        });

        if (tickets.length === 0) {
          job = this.database.updateImportJob(jobId, {
            status: "completed",
            status_message: "Finished: no more tickets returned by the API.",
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[job] job ${jobId} completed because the API returned no more tickets`);
          return job;
        }

        for (let index = job.nextTicketOffset; index < tickets.length; index += 1) {
          const ticket = tickets[index];
          this.logger?.log(
            `[job] processing ticket ${index + 1}/${tickets.length} on page ${job.currentPage}: ${ticket.uuid}`
          );

          this.database.updateImportJob(jobId, {
            status: "running",
            current_page: job.currentPage,
            next_ticket_offset: index,
            current_ticket_uuid: ticket.uuid,
            status_message: `Processing ticket ${ticket.uuid}`
          });

          try {
            await this.importService.importSingleTicket(ticket, { persist: job.persist });
            job = this.database.updateImportJob(jobId, {
              current_page: job.currentPage,
              next_ticket_offset: index + 1,
              current_ticket_uuid: null,
              tickets_seen: job.ticketsSeen + 1,
              tickets_imported: job.ticketsImported + 1,
              status_message: `Imported ticket ${ticket.uuid}`
            });
          } catch (error) {
            const message = error instanceof Error ? error.message : "Unknown error";
            this.database.addImportJobError(jobId, {
              ticketId: ticket.id,
              ticketUuid: ticket.uuid,
              errorMessage: message
            });
            job = this.database.updateImportJob(jobId, {
              current_page: job.currentPage,
              next_ticket_offset: index + 1,
              current_ticket_uuid: null,
              tickets_seen: job.ticketsSeen + 1,
              tickets_failed: job.ticketsFailed + 1,
              status_message: `Failed ticket ${ticket.uuid}`
            });
            this.logger?.error?.(`[job] failed ticket ${ticket.uuid} in job ${jobId}: ${message}`);
          }
        }

        const pagesProcessed = job.pagesProcessed + 1;
        const nextPage = job.currentPage + 1;
        job = this.database.updateImportJob(jobId, {
          pages_processed: pagesProcessed,
          current_page: nextPage,
          next_ticket_offset: 0,
          current_ticket_uuid: null,
          status_message: `Completed page ${job.currentPage}`
        });

        const reachedRequestedPages = !job.sweepAll && job.pagesRequested && pagesProcessed >= job.pagesRequested;
        const reachedAllPages = job.sweepAll && totalPages && job.currentPage > totalPages;
        const reachedMaxPages = job.maxPages && pagesProcessed >= job.maxPages;

        if (reachedRequestedPages || reachedAllPages) {
          const message = reachedAllPages
            ? "Finished all pages."
            : `Finished requested pages (${job.pagesRequested}).`;
          job = this.database.updateImportJob(jobId, {
            status: "completed",
            status_message: message,
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[job] job ${jobId} completed`);
          return job;
        }

        if (reachedMaxPages) {
          job = this.database.updateImportJob(jobId, {
            status: "paused",
            status_message: `Paused after reaching maxPages=${job.maxPages}.`,
            finished_at: new Date().toISOString()
          });
          this.logger?.log(`[job] job ${jobId} paused after reaching maxPages=${job.maxPages}`);
          return job;
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      job = this.database.updateImportJob(jobId, {
        status: "failed",
        status_message: message,
        finished_at: new Date().toISOString()
      });
      this.logger?.error?.(`[job] job ${jobId} failed: ${message}`);
      return job;
    }
  }
}
