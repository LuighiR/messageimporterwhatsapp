import { buildTicketImportSnapshot } from "./normalize.mjs";

export class TicketImportService {
  constructor({ client, database = null, logger = console }) {
    this.client = client;
    this.database = database;
    this.logger = logger;
  }

  async buildSnapshotFromTicket(ticket) {
    this.logger?.log(`[import] fetching full conversation for ticket ${ticket.uuid} (${ticket.id})`);
    const messagesPayload = await this.client.getAllMessagesByTicketUuid(ticket.uuid, { pageSize: 100 });
    const snapshot = buildTicketImportSnapshot(ticket, messagesPayload.messages);
    this.logger?.log(
      `[import] snapshot ready for ticket ${ticket.uuid}: ${snapshot.sessionCount} sessions, ${snapshot.messageCount} messages`
    );
    return snapshot;
  }

  async importSingleTicket(ticket, { persist = true } = {}) {
    const snapshot = await this.buildSnapshotFromTicket(ticket);
    const persisted = persist && this.database ? this.database.persistSnapshot(snapshot) : null;

    return {
      ticketId: ticket.id,
      ticketUuid: ticket.uuid,
      sessionCount: snapshot.sessionCount,
      messageCount: snapshot.messageCount,
      persisted: Boolean(persisted),
      persistedAt: persisted?.importedAt || null,
      snapshot
    };
  }

  async importTicketPages({
    page = 1,
    limit = 5,
    pages = 1,
    persist = true,
    sweepAll = false,
    maxPages = null,
    resultSampleLimit = 100
  } = {}) {
    const startedAt = new Date().toISOString();
    const results = [];
    const errors = [];
    let pagesProcessed = 0;
    let ticketsSeen = 0;
    let ticketsFailed = 0;
    let totalPages = null;
    let currentPage = page;

    while (true) {
      this.logger?.log(`[batch] fetching tickets page ${currentPage} with limit ${limit}`);
      const ticketsPayload = await this.client.listTickets({ page: currentPage, limit });
      const tickets = ticketsPayload.data || [];
      totalPages = Number(ticketsPayload.pagination?.totalPages || totalPages || 0) || null;
      pagesProcessed += 1;
      ticketsSeen += tickets.length;

      if (totalPages) {
        this.logger?.log(`[batch] page ${currentPage}/${totalPages} returned ${tickets.length} ticket(s)`);
      } else {
        this.logger?.log(`[batch] page ${currentPage} returned ${tickets.length} ticket(s)`);
      }

      for (let index = 0; index < tickets.length; index += 1) {
        const ticket = tickets[index];
        const position = `${index + 1}/${tickets.length}`;
        this.logger?.log(`[batch] processing ticket ${position} on page ${currentPage}: ${ticket.uuid}`);

        try {
          const imported = await this.importSingleTicket(ticket, { persist });
          if (results.length < resultSampleLimit) {
            results.push({
              ticketId: imported.ticketId,
              ticketUuid: imported.ticketUuid,
              sessionCount: imported.sessionCount,
              messageCount: imported.messageCount,
              persisted: imported.persisted,
              persistedAt: imported.persistedAt
            });
          }
        } catch (error) {
          const message = error instanceof Error ? error.message : "Unknown error";
          ticketsFailed += 1;
          this.logger?.error?.(`[batch] failed ticket ${ticket.uuid}: ${message}`);
          if (errors.length < resultSampleLimit) {
            errors.push({
              ticketId: ticket.id,
              ticketUuid: ticket.uuid,
              error: message
            });
          }
        }
      }

      if (tickets.length === 0) {
        break;
      }

      if (!sweepAll && pagesProcessed >= pages) {
        break;
      }

      if (sweepAll && totalPages && currentPage >= totalPages) {
        break;
      }

      if (maxPages && pagesProcessed >= maxPages) {
        this.logger?.log(`[batch] stopping early after reaching maxPages=${maxPages}`);
        break;
      }

      currentPage += 1;
    }

    const finishedAt = new Date().toISOString();
    this.logger?.log(
      `[batch] completed import: ${ticketsSeen - ticketsFailed} ticket(s) processed successfully, ${ticketsFailed} failed, ${pagesProcessed} page(s) processed`
    );

    return {
      startedAt,
      finishedAt,
      page,
      totalPages,
      sweepAll,
      pagesRequested: pages,
      pagesProcessed,
      maxPages,
      ticketLimit: limit,
      ticketsSeen,
      ticketsImported: ticketsSeen - ticketsFailed,
      ticketsFailed,
      persist,
      resultSampleLimit,
      resultsTruncated: ticketsSeen - ticketsFailed > results.length,
      errorsTruncated: ticketsFailed > errors.length,
      results,
      errors
    };
  }
}
