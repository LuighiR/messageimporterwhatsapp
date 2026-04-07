import http from "node:http";
import { getConfig } from "./config.mjs";
import { CorzClient } from "./corzClient.mjs";
import { ContactSyncJobRunner } from "./contactSyncRunner.mjs";
import { CoreContactSyncJobRunner } from "./coreContactSyncJobRunner.mjs";
import { CoreImportJobRunner } from "./coreImportJobRunner.mjs";
import { CoreRepository } from "./coreRepository.mjs";
import { ImporterDatabase } from "./database.mjs";
import { TicketImportService } from "./importService.mjs";
import { ImportJobRunner } from "./jobRunner.mjs";
import { normalizeTicket } from "./normalize.mjs";
import { SlidingWindowRateLimiter } from "./rateLimiter.mjs";

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload, null, 2));
}

function sendUnauthorized(response) {
  sendJson(response, 401, {
    error: "Unauthorized",
    message: "Provide the x-job-key header with a valid key."
  });
}

function sendMissingJobKey(response) {
  sendJson(response, 500, {
    error: "Server misconfigured",
    message: "JOB_KEY is not configured on the server."
  });
}

function sendNotFound(response) {
  sendJson(response, 404, {
    error: "Not found",
    availableRoutes: [
      "GET /health",
      "GET /import/tickets?page=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
      "GET /import/tickets/:ticketUuid/messages?page=1&limit=20",
      "GET /import/tickets/:ticketUuid/snapshot?ticketPage=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
      "POST /import/tickets/:ticketUuid/persist?ticketPage=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
      "POST /import/batch?page=1&ticketLimit=100&pages=1&startDate=2026-04-01&endDate=2026-04-07",
      "POST /import/batch?page=1&ticketLimit=100&pages=all&maxPages=10&startDate=2026-04-01&endDate=2026-04-07",
      "POST /jobs/imports/start?page=1&ticketLimit=100&pages=all&startDate=2026-04-01&endDate=2026-04-07",
      "POST /jobs/imports/:jobId/resume",
      "GET /jobs/imports?limit=20",
      "GET /jobs/imports/:jobId",
      "POST /jobs/contacts/start?page=1&contactLimit=100&pages=all",
      "POST /jobs/contacts/:jobId/resume",
      "GET /jobs/contacts?limit=20",
      "GET /jobs/contacts/:jobId",
      "GET /db/tickets?limit=50",
      "GET /db/tickets/:ticketUuid",
      "GET /db/contacts?limit=50",
      "GET /db/contacts/:contactId",
      "GET /core/clients",
      "POST /core/jobs/imports/start?clientId=ferracosul&page=1&ticketLimit=100&pages=all&startDate=2026-04-01&endDate=2026-04-07",
      "GET /core/jobs/imports/:jobId",
      "POST /core/jobs/imports/:jobId/resume",
      "POST /core/jobs/contacts/start?clientId=ferracosul&page=1&contactLimit=100&pages=all",
      "GET /core/jobs/contacts/:jobId",
      "POST /core/jobs/contacts/:jobId/resume",
      "GET /core/db/tickets?clientId=ferracosul&limit=50",
      "GET /core/db/contacts?clientId=ferracosul&limit=50",
      "GET /core/db/contacts/:contactId?clientId=ferracosul"
    ]
  });
}

function getPagination(searchParams, defaultLimit, maxLimit = null) {
  const page = Number.parseInt(searchParams.get("page") || "1", 10);
  const requestedLimit = Number.parseInt(searchParams.get("limit") || `${defaultLimit}`, 10);
  const safeLimit = Number.isNaN(requestedLimit) || requestedLimit < 1 ? defaultLimit : requestedLimit;

  return {
    page: Number.isNaN(page) || page < 1 ? 1 : page,
    limit: maxLimit ? Math.min(safeLimit, maxLimit) : safeLimit
  };
}

function getTicketDateFilters(searchParams) {
  const readFilter = (key) => {
    const value = searchParams.get(key);
    return value && value.trim() ? value.trim() : null;
  };

  return {
    startDate: readFilter("startDate"),
    endDate: readFilter("endDate")
  };
}

function pickTicketByUuid(payload, ticketUuid) {
  return (payload.data || []).find((ticket) => ticket.uuid === ticketUuid) || null;
}

function isProtectedPath(pathname) {
  if (pathname === "/" || pathname === "/health") {
    return false;
  }

  return (
    pathname.startsWith("/import/") ||
    pathname.startsWith("/jobs/") ||
    pathname.startsWith("/db/") ||
    pathname.startsWith("/core/")
  );
}

function readJobKeyHeader(headers) {
  const value = headers["x-job-key"];

  if (Array.isArray(value)) {
    return value[0] || null;
  }

  return value || null;
}

async function createServer() {
  const config = getConfig();
  const rateLimiter = new SlidingWindowRateLimiter({
    maxRequests: config.rateLimitRequests,
    windowMs: config.rateLimitWindowMs
  });
  const client = new CorzClient({
    ...config,
    rateLimiter,
    logger: console
  });
  const database = new ImporterDatabase(config.databasePath, { logger: console });
  const importService = new TicketImportService({
    client,
    database,
    logger: console
  });
  const jobRunner = new ImportJobRunner({
    client,
    importService,
    database,
    logger: console
  });
  const contactSyncRunner = new ContactSyncJobRunner({
    client,
    database,
    logger: console
  });
  const coreRateLimiters = new Map();
  const createTenantApiClient = (clientConfig) => {
    if (!coreRateLimiters.has(clientConfig.id)) {
      coreRateLimiters.set(
        clientConfig.id,
        new SlidingWindowRateLimiter({
          maxRequests: config.rateLimitRequests,
          windowMs: config.rateLimitWindowMs
        })
      );
    }

    return new CorzClient({
      baseUrl: clientConfig.api_base_url,
      apiKey: clientConfig.api_key,
      rateLimiter: coreRateLimiters.get(clientConfig.id),
      logger: console
    });
  };
  let coreRepository = null;
  let coreImportJobRunner = null;
  let coreContactSyncJobRunner = null;

  if (config.postgresUrl) {
    coreRepository = new CoreRepository({
      connectionString: config.postgresUrl,
      schema: config.postgresSchema,
      logger: console
    });
    await coreRepository.init();
    coreImportJobRunner = new CoreImportJobRunner({
      repository: coreRepository,
      createApiClient: createTenantApiClient,
      logger: console
    });
    coreContactSyncJobRunner = new CoreContactSyncJobRunner({
      repository: coreRepository,
      createApiClient: createTenantApiClient,
      logger: console
    });
  }

  return http.createServer(async (request, response) => {
    if (!request.url) {
      return sendNotFound(response);
    }

    const url = new URL(request.url, `http://${request.headers.host || "localhost"}`);
    const { pathname, searchParams } = url;
    const startedAt = Date.now();
    console.log(`[http] ${request.method} ${pathname}${url.search}`);

    try {
      if (isProtectedPath(pathname)) {
        if (!config.jobKey) {
          return sendMissingJobKey(response);
        }

        const providedJobKey = readJobKeyHeader(request.headers);
        if (providedJobKey !== config.jobKey) {
          return sendUnauthorized(response);
        }
      }

      if (pathname === "/" && request.method === "GET") {
        return sendJson(response, 200, {
          service: "dkw-message-importer-test",
          status: "running",
          quickStart: [
            "GET /health",
            "GET /import/tickets?page=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
            "GET /import/tickets/:ticketUuid/messages?page=1&limit=20",
            "GET /import/tickets/:ticketUuid/snapshot?ticketPage=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
            "POST /import/tickets/:ticketUuid/persist?ticketPage=1&ticketLimit=100&startDate=2026-04-01&endDate=2026-04-07",
            "POST /import/batch?page=1&ticketLimit=100&pages=1&startDate=2026-04-01&endDate=2026-04-07",
            "POST /import/batch?page=1&ticketLimit=100&pages=all&maxPages=10&startDate=2026-04-01&endDate=2026-04-07",
            "POST /jobs/imports/start?page=1&ticketLimit=100&pages=all&startDate=2026-04-01&endDate=2026-04-07",
            "POST /jobs/imports/:jobId/resume",
            "GET /jobs/imports?limit=20",
            "GET /jobs/imports/:jobId",
            "POST /jobs/contacts/start?page=1&contactLimit=100&pages=all",
            "POST /jobs/contacts/:jobId/resume",
            "GET /jobs/contacts?limit=20",
            "GET /jobs/contacts/:jobId",
            "GET /db/tickets?limit=50",
            "GET /db/tickets/:ticketUuid",
            "GET /db/contacts?limit=50",
            "GET /db/contacts/:contactId",
            "GET /core/clients",
            "POST /core/jobs/imports/start?clientId=ferracosul&page=1&ticketLimit=100&pages=all&startDate=2026-04-01&endDate=2026-04-07",
            "GET /core/jobs/imports/:jobId",
            "POST /core/jobs/imports/:jobId/resume",
            "POST /core/jobs/contacts/start?clientId=ferracosul&page=1&contactLimit=100&pages=all",
            "GET /core/jobs/contacts/:jobId",
            "POST /core/jobs/contacts/:jobId/resume",
            "GET /core/db/tickets?clientId=ferracosul&limit=50",
            "GET /core/db/contacts?clientId=ferracosul&limit=50",
            "GET /core/db/contacts/:contactId?clientId=ferracosul"
          ]
        });
      }

      if (pathname === "/health") {
        return sendJson(response, 200, {
          status: "ok",
          databasePath: config.databasePath,
          postgresEnabled: Boolean(config.postgresUrl),
          externalApiRateLimit: {
            maxRequests: config.rateLimitRequests,
            windowMs: config.rateLimitWindowMs
          }
        });
      }

      if (pathname === "/import/tickets" && request.method === "GET") {
        const ticketsSearchParams = new URLSearchParams(searchParams);
        if (searchParams.has("ticketLimit") && !searchParams.has("limit")) {
          ticketsSearchParams.set("limit", searchParams.get("ticketLimit"));
        }
        const { page, limit } = getPagination(ticketsSearchParams, 100);
        const { startDate, endDate } = getTicketDateFilters(searchParams);
        const payload = await client.listTickets({ page, limit, startDate, endDate });

        return sendJson(response, 200, {
          filters: {
            startDate,
            endDate
          },
          pagination: payload.pagination,
          tickets: (payload.data || []).map((ticket) => ({
            ticket: normalizeTicket(ticket),
            sessions: (ticket.ticketTrakings || []).map((tracking, index, trackings) => ({
              sessionId: tracking.id,
              sequence: index + 1,
              openedAt: tracking.createdAt,
              startedAt: tracking.startedAt,
              finishedAt: tracking.finishedAt,
              nextSessionOpenedAt: trackings[index + 1]?.createdAt || null,
              user: tracking.user
                ? {
                    id: tracking.user.id,
                    name: tracking.user.name,
                    email: tracking.user.email
                  }
                : null
            }))
          }))
        });
      }

      const messageMatch = pathname.match(/^\/import\/tickets\/([^/]+)\/messages$/);
      if (messageMatch && request.method === "GET") {
        const [, ticketUuid] = messageMatch;
        const { page, limit } = getPagination(searchParams, 20, 20);
        const payload = await client.listMessagesByTicketUuid(ticketUuid, { page, limit });

        return sendJson(response, 200, payload);
      }

      const snapshotMatch = pathname.match(/^\/import\/tickets\/([^/]+)\/snapshot$/);
      if (snapshotMatch && request.method === "GET") {
        const [, ticketUuid] = snapshotMatch;
        const ticketPage = Number.parseInt(searchParams.get("ticketPage") || searchParams.get("page") || "1", 10);
        const ticketLimit = Number.parseInt(searchParams.get("ticketLimit") || "100", 10);
        const { startDate, endDate } = getTicketDateFilters(searchParams);
        console.log(`[import] building snapshot for ticket ${ticketUuid} using ticketPage=${ticketPage} ticketLimit=${ticketLimit}`);

        const ticketsPayload = await client.listTickets({
          page: ticketPage,
          limit: ticketLimit,
          startDate,
          endDate
        });
        const ticket = pickTicketByUuid(ticketsPayload, ticketUuid);

        if (!ticket) {
          return sendJson(response, 404, {
            error: "Ticket not found in the requested ticket page.",
            tip: "Increase ticketLimit or change ticketPage when testing older tickets."
          });
        }

        const snapshot = await importService.buildSnapshotFromTicket(ticket);

        return sendJson(response, 200, snapshot);
      }

      const persistMatch = pathname.match(/^\/import\/tickets\/([^/]+)\/persist$/);
      if (persistMatch && request.method === "POST") {
        const [, ticketUuid] = persistMatch;
        const ticketPage = Number.parseInt(searchParams.get("ticketPage") || "1", 10);
        const ticketLimit = Number.parseInt(searchParams.get("ticketLimit") || "100", 10);
        const { startDate, endDate } = getTicketDateFilters(searchParams);
        console.log(`[import] persisting ticket ${ticketUuid} using ticketPage=${ticketPage} ticketLimit=${ticketLimit}`);

        const ticketsPayload = await client.listTickets({
          page: ticketPage,
          limit: ticketLimit,
          startDate,
          endDate
        });
        const ticket = pickTicketByUuid(ticketsPayload, ticketUuid);

        if (!ticket) {
          return sendJson(response, 404, {
            error: "Ticket not found in the requested ticket page.",
            tip: "Increase ticketLimit or change ticketPage when testing older tickets."
          });
        }

        const imported = await importService.importSingleTicket(ticket, { persist: true });
        console.log(
          `[import] persistence complete for ticket ${ticketUuid}: ${imported.sessionCount} sessions, ${imported.messageCount} messages`
        );

        return sendJson(response, 200, {
          persisted: {
            ticketId: imported.ticketId,
            ticketUuid: imported.ticketUuid,
            sessionCount: imported.sessionCount,
            messageCount: imported.messageCount,
            importedAt: imported.persistedAt
          },
          snapshot: imported.snapshot
        });
      }

      if (pathname === "/import/batch" && request.method === "POST") {
        const page = Number.parseInt(searchParams.get("page") || "1", 10);
        const ticketLimit = Number.parseInt(
          searchParams.get("ticketLimit") || searchParams.get("limit") || "100",
          10
        );
        const rawPages = searchParams.get("pages") || "1";
        const pages = rawPages === "all" ? null : Number.parseInt(rawPages, 10);
        const maxPages = Number.parseInt(searchParams.get("maxPages") || "0", 10);
        const persist = searchParams.get("persist") !== "false";
        const sweepAll = rawPages === "all";
        const { startDate, endDate } = getTicketDateFilters(searchParams);
        console.log(
          `[batch] starting automatic flow page=${page} ticketLimit=${ticketLimit} pages=${rawPages} maxPages=${maxPages || "none"} persist=${persist}`
        );

        const result = await importService.importTicketPages({
          page: Number.isNaN(page) || page < 1 ? 1 : page,
          limit: Number.isNaN(ticketLimit) || ticketLimit < 1 ? 100 : ticketLimit,
          pages: sweepAll || Number.isNaN(pages) || pages < 1 ? 1 : pages,
          startDate,
          endDate,
          sweepAll,
          maxPages: Number.isNaN(maxPages) || maxPages < 1 ? null : maxPages,
          persist
        });

        return sendJson(response, 200, result);
      }

      if (pathname === "/jobs/imports/start" && request.method === "POST") {
        const page = Number.parseInt(searchParams.get("page") || "1", 10);
        const ticketLimit = Number.parseInt(
          searchParams.get("ticketLimit") || searchParams.get("limit") || "100",
          10
        );
        const rawPages = searchParams.get("pages") || "all";
        const pages = rawPages === "all" ? null : Number.parseInt(rawPages, 10);
        const maxPages = Number.parseInt(searchParams.get("maxPages") || "0", 10);
        const persist = searchParams.get("persist") !== "false";
        const sweepAll = rawPages === "all";
        const { startDate, endDate } = getTicketDateFilters(searchParams);

        const job = jobRunner.startJob({
          page: Number.isNaN(page) || page < 1 ? 1 : page,
          limit: Number.isNaN(ticketLimit) || ticketLimit < 1 ? 100 : ticketLimit,
          pages: sweepAll || Number.isNaN(pages) || pages < 1 ? 1 : pages,
          startDate,
          endDate,
          sweepAll,
          maxPages: Number.isNaN(maxPages) || maxPages < 1 ? null : maxPages,
          persist
        });

        return sendJson(response, 202, job);
      }

      if (pathname === "/jobs/imports" && request.method === "GET") {
        const limit = Number.parseInt(searchParams.get("limit") || "20", 10);
        return sendJson(response, 200, {
          jobs: jobRunner.listJobs(Number.isNaN(limit) ? 20 : limit)
        });
      }

      const importJobMatch = pathname.match(/^\/jobs\/imports\/(\d+)$/);
      if (importJobMatch && request.method === "GET") {
        const [, jobIdText] = importJobMatch;
        const job = jobRunner.getJob(Number.parseInt(jobIdText, 10));

        if (!job) {
          return sendJson(response, 404, {
            error: "Import job not found."
          });
        }

        return sendJson(response, 200, job);
      }

      const resumeJobMatch = pathname.match(/^\/jobs\/imports\/(\d+)\/resume$/);
      if (resumeJobMatch && request.method === "POST") {
        const [, jobIdText] = resumeJobMatch;
        const rawMaxPages = searchParams.get("maxPages");
        const parsedMaxPages = rawMaxPages === null ? undefined : Number.parseInt(rawMaxPages, 10);
        const job = jobRunner.resumeJob(Number.parseInt(jobIdText, 10), {
          maxPages:
            rawMaxPages === null
              ? undefined
              : Number.isNaN(parsedMaxPages) || parsedMaxPages < 1
                ? null
                : parsedMaxPages
        });
        return sendJson(response, 202, job);
      }

      if (pathname === "/jobs/contacts/start" && request.method === "POST") {
        const page = Number.parseInt(searchParams.get("page") || "1", 10);
        const contactLimit = Number.parseInt(
          searchParams.get("contactLimit") || searchParams.get("limit") || "100",
          10
        );
        const rawPages = searchParams.get("pages") || "all";
        const pages = rawPages === "all" ? null : Number.parseInt(rawPages, 10);
        const maxPages = Number.parseInt(searchParams.get("maxPages") || "0", 10);
        const sweepAll = rawPages === "all";

        const job = contactSyncRunner.startJob({
          page: Number.isNaN(page) || page < 1 ? 1 : page,
          limit: Number.isNaN(contactLimit) || contactLimit < 1 ? 100 : contactLimit,
          pages: sweepAll || Number.isNaN(pages) || pages < 1 ? 1 : pages,
          sweepAll,
          maxPages: Number.isNaN(maxPages) || maxPages < 1 ? null : maxPages
        });

        return sendJson(response, 202, job);
      }

      if (pathname === "/jobs/contacts" && request.method === "GET") {
        const limit = Number.parseInt(searchParams.get("limit") || "20", 10);
        return sendJson(response, 200, {
          jobs: contactSyncRunner.listJobs(Number.isNaN(limit) ? 20 : limit)
        });
      }

      const contactJobMatch = pathname.match(/^\/jobs\/contacts\/(\d+)$/);
      if (contactJobMatch && request.method === "GET") {
        const [, jobIdText] = contactJobMatch;
        const job = contactSyncRunner.getJob(Number.parseInt(jobIdText, 10));

        if (!job) {
          return sendJson(response, 404, {
            error: "Contact sync job not found."
          });
        }

        return sendJson(response, 200, job);
      }

      const resumeContactJobMatch = pathname.match(/^\/jobs\/contacts\/(\d+)\/resume$/);
      if (resumeContactJobMatch && request.method === "POST") {
        const [, jobIdText] = resumeContactJobMatch;
        const rawMaxPages = searchParams.get("maxPages");
        const parsedMaxPages = rawMaxPages === null ? undefined : Number.parseInt(rawMaxPages, 10);
        const job = contactSyncRunner.resumeJob(Number.parseInt(jobIdText, 10), {
          maxPages:
            rawMaxPages === null
              ? undefined
              : Number.isNaN(parsedMaxPages) || parsedMaxPages < 1
                ? null
                : parsedMaxPages
        });
        return sendJson(response, 202, job);
      }

      if (pathname === "/db/tickets" && request.method === "GET") {
        const limit = Number.parseInt(searchParams.get("limit") || "50", 10);
        return sendJson(response, 200, {
          tickets: database.listTickets(Number.isNaN(limit) ? 50 : limit)
        });
      }

      const persistedTicketMatch = pathname.match(/^\/db\/tickets\/([^/]+)$/);
      if (persistedTicketMatch && request.method === "GET") {
        const [, ticketUuid] = persistedTicketMatch;
        const snapshot = database.getTicketSnapshot(ticketUuid);

        if (!snapshot) {
          return sendJson(response, 404, {
            error: "Ticket not found in SQLite database."
          });
        }

        return sendJson(response, 200, snapshot);
      }

      if (pathname === "/db/contacts" && request.method === "GET") {
        const limit = Number.parseInt(searchParams.get("limit") || "50", 10);
        return sendJson(response, 200, {
          contacts: database.listContacts(Number.isNaN(limit) ? 50 : limit)
        });
      }

      const persistedContactMatch = pathname.match(/^\/db\/contacts\/(\d+)$/);
      if (persistedContactMatch && request.method === "GET") {
        const [, contactIdText] = persistedContactMatch;
        const contact = database.getContact(Number.parseInt(contactIdText, 10));

        if (!contact) {
          return sendJson(response, 404, {
            error: "Contact not found in SQLite database."
          });
        }

        return sendJson(response, 200, contact);
      }

      if (pathname.startsWith("/core/")) {
        if (!coreRepository || !coreImportJobRunner || !coreContactSyncJobRunner) {
          return sendJson(response, 500, {
            error: "Postgres integration is not configured. Set POSTGRES_URL in the root .env file."
          });
        }

        if (pathname === "/core/clients" && request.method === "GET") {
          const clients = await coreRepository.listActiveClients();
          return sendJson(response, 200, { clients });
        }

        if (pathname === "/core/jobs/imports/start" && request.method === "POST") {
          const clientId = searchParams.get("clientId");
          const page = Number.parseInt(searchParams.get("page") || "1", 10);
          const ticketLimit = Number.parseInt(searchParams.get("ticketLimit") || "100", 10);
          const rawPages = searchParams.get("pages") || "all";
          const pages = rawPages === "all" ? null : Number.parseInt(rawPages, 10);
          const maxPages = Number.parseInt(searchParams.get("maxPages") || "0", 10);
          const persist = searchParams.get("persist") !== "false";
          const sweepAll = rawPages === "all";
          const { startDate, endDate } = getTicketDateFilters(searchParams);

          if (!clientId) {
            return sendJson(response, 400, { error: "clientId is required." });
          }

          const job = await coreImportJobRunner.startJob({
            clientId,
            page: Number.isNaN(page) || page < 1 ? 1 : page,
            limit: Number.isNaN(ticketLimit) || ticketLimit < 1 ? 100 : ticketLimit,
            pages: sweepAll || Number.isNaN(pages) || pages < 1 ? 1 : pages,
            startDate,
            endDate,
            sweepAll,
            maxPages: Number.isNaN(maxPages) || maxPages < 1 ? null : maxPages,
            persist
          });

          return sendJson(response, 202, job);
        }

        if (pathname === "/core/jobs/imports" && request.method === "GET") {
          const limit = Number.parseInt(searchParams.get("limit") || "20", 10);
          const clientId = searchParams.get("clientId");
          return sendJson(response, 200, {
            jobs: await coreRepository.listImportJobs(Number.isNaN(limit) ? 20 : limit, clientId || null)
          });
        }

        const coreImportJobMatch = pathname.match(/^\/core\/jobs\/imports\/(\d+)$/);
        if (coreImportJobMatch && request.method === "GET") {
          const [, jobIdText] = coreImportJobMatch;
          const job = await coreImportJobRunner.getJob(Number.parseInt(jobIdText, 10));
          if (!job) {
            return sendJson(response, 404, { error: "Core import job not found." });
          }

          return sendJson(response, 200, job);
        }

        const coreImportResumeMatch = pathname.match(/^\/core\/jobs\/imports\/(\d+)\/resume$/);
        if (coreImportResumeMatch && request.method === "POST") {
          const [, jobIdText] = coreImportResumeMatch;
          const rawMaxPages = searchParams.get("maxPages");
          const parsedMaxPages = rawMaxPages === null ? undefined : Number.parseInt(rawMaxPages, 10);
          const job = await coreImportJobRunner.resumeJob(Number.parseInt(jobIdText, 10), {
            maxPages:
              rawMaxPages === null
                ? undefined
                : Number.isNaN(parsedMaxPages) || parsedMaxPages < 1
                  ? null
                  : parsedMaxPages
          });

          return sendJson(response, 202, job);
        }

        if (pathname === "/core/jobs/contacts/start" && request.method === "POST") {
          const clientId = searchParams.get("clientId");
          const page = Number.parseInt(searchParams.get("page") || "1", 10);
          const contactLimit = Number.parseInt(searchParams.get("contactLimit") || "100", 10);
          const rawPages = searchParams.get("pages") || "all";
          const pages = rawPages === "all" ? null : Number.parseInt(rawPages, 10);
          const maxPages = Number.parseInt(searchParams.get("maxPages") || "0", 10);
          const sweepAll = rawPages === "all";

          if (!clientId) {
            return sendJson(response, 400, { error: "clientId is required." });
          }

          const job = await coreContactSyncJobRunner.startJob({
            clientId,
            page: Number.isNaN(page) || page < 1 ? 1 : page,
            limit: Number.isNaN(contactLimit) || contactLimit < 1 ? 100 : contactLimit,
            pages: sweepAll || Number.isNaN(pages) || pages < 1 ? 1 : pages,
            sweepAll,
            maxPages: Number.isNaN(maxPages) || maxPages < 1 ? null : maxPages
          });

          return sendJson(response, 202, job);
        }

        if (pathname === "/core/jobs/contacts" && request.method === "GET") {
          const limit = Number.parseInt(searchParams.get("limit") || "20", 10);
          const clientId = searchParams.get("clientId");
          return sendJson(response, 200, {
            jobs: await coreRepository.listContactSyncJobs(Number.isNaN(limit) ? 20 : limit, clientId || null)
          });
        }

        const coreContactJobMatch = pathname.match(/^\/core\/jobs\/contacts\/(\d+)$/);
        if (coreContactJobMatch && request.method === "GET") {
          const [, jobIdText] = coreContactJobMatch;
          const job = await coreContactSyncJobRunner.getJob(Number.parseInt(jobIdText, 10));
          if (!job) {
            return sendJson(response, 404, { error: "Core contact sync job not found." });
          }

          return sendJson(response, 200, job);
        }

        const coreContactResumeMatch = pathname.match(/^\/core\/jobs\/contacts\/(\d+)\/resume$/);
        if (coreContactResumeMatch && request.method === "POST") {
          const [, jobIdText] = coreContactResumeMatch;
          const rawMaxPages = searchParams.get("maxPages");
          const parsedMaxPages = rawMaxPages === null ? undefined : Number.parseInt(rawMaxPages, 10);
          const job = await coreContactSyncJobRunner.resumeJob(Number.parseInt(jobIdText, 10), {
            maxPages:
              rawMaxPages === null
                ? undefined
                : Number.isNaN(parsedMaxPages) || parsedMaxPages < 1
                  ? null
                  : parsedMaxPages
          });

          return sendJson(response, 202, job);
        }

        if (pathname === "/core/db/tickets" && request.method === "GET") {
          const clientId = searchParams.get("clientId");
          const limit = Number.parseInt(searchParams.get("limit") || "50", 10);

          if (!clientId) {
            return sendJson(response, 400, { error: "clientId is required." });
          }

          return sendJson(response, 200, {
            tickets: await coreRepository.listTickets(clientId, Number.isNaN(limit) ? 50 : limit)
          });
        }

        if (pathname === "/core/db/contacts" && request.method === "GET") {
          const clientId = searchParams.get("clientId");
          const limit = Number.parseInt(searchParams.get("limit") || "50", 10);

          if (!clientId) {
            return sendJson(response, 400, { error: "clientId is required." });
          }

          return sendJson(response, 200, {
            contacts: await coreRepository.listContacts(clientId, Number.isNaN(limit) ? 50 : limit)
          });
        }

        const coreDbContactMatch = pathname.match(/^\/core\/db\/contacts\/(\d+)$/);
        if (coreDbContactMatch && request.method === "GET") {
          const [, contactIdText] = coreDbContactMatch;
          const clientId = searchParams.get("clientId");

          if (!clientId) {
            return sendJson(response, 400, { error: "clientId is required." });
          }

          const contact = await coreRepository.getContact(clientId, Number.parseInt(contactIdText, 10));
          if (!contact) {
            return sendJson(response, 404, { error: "Core contact not found." });
          }

          return sendJson(response, 200, contact);
        }
      }

      return sendNotFound(response);
    } catch (error) {
      console.error(`[http] error on ${request.method} ${pathname}: ${error instanceof Error ? error.message : error}`);
      return sendJson(response, 500, {
        error: error instanceof Error ? error.message : "Unknown error"
      });
    } finally {
      console.log(`[http] completed ${request.method} ${pathname} in ${Date.now() - startedAt}ms`);
    }
  }).listen(config.port, () => {
    console.log(`Message importer test service listening on http://localhost:${config.port}`);
  });
}

createServer().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
