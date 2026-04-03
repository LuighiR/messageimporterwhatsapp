const MAX_MESSAGES_PER_PAGE = 20;
const MAX_CONTACTS_PER_PAGE = 100;

function joinUrl(baseUrl, pathname, params = {}) {
  const url = new URL(pathname, baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, `${value}`);
    }
  }

  return url;
}

export class CorzClient {
  constructor({ baseUrl, apiKey, fetchImpl = fetch, rateLimiter = null, logger = console }) {
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
    this.fetchImpl = fetchImpl;
    this.rateLimiter = rateLimiter;
    this.logger = logger;
  }

  async request(pathname, params = {}) {
    if (this.rateLimiter) {
      await this.rateLimiter.acquire();
    }

    const url = joinUrl(this.baseUrl, pathname, params);
    this.logger?.log(`[corz] request ${url.pathname}${url.search}`);
    const response = await this.fetchImpl(url, {
      headers: {
        "api-key": this.apiKey,
        accept: "application/json"
      }
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Corz API error ${response.status} for ${url.pathname}: ${body}`);
    }

    return response.json();
  }

  async listTickets({ page = 1, limit = 20 } = {}) {
    return this.request("ticket", { page, limit });
  }

  async listMessagesByTicketUuid(ticketUuid, { page = 1, limit = 100 } = {}) {
    return this.request(`ticket/${ticketUuid}/messages`, {
      page,
      limit: Math.min(limit, MAX_MESSAGES_PER_PAGE)
    });
  }

  async listContacts({ page = 1, limit = 100, id = null } = {}) {
    return this.request("contact", {
      page,
      limit: Math.min(limit, MAX_CONTACTS_PER_PAGE),
      id
    });
  }

  async getAllMessagesByTicketUuid(ticketUuid, { pageSize = 100 } = {}) {
    const normalizedPageSize = Math.min(pageSize, MAX_MESSAGES_PER_PAGE);
    this.logger?.log(
      `[corz] fetching all messages for ticket ${ticketUuid} with page size ${normalizedPageSize}`
    );
    const firstPage = await this.listMessagesByTicketUuid(ticketUuid, {
      page: 1,
      limit: normalizedPageSize
    });
    const messages = [...(firstPage.messages || [])];
    const totalPages = Number(firstPage.totalPages || 1);
    this.logger?.log(
      `[corz] messages page 1/${totalPages} for ticket ${ticketUuid}, received ${messages.length} items so far`
    );

    for (let page = 2; page <= totalPages; page += 1) {
      const nextPage = await this.listMessagesByTicketUuid(ticketUuid, {
        page,
        limit: normalizedPageSize
      });
      messages.push(...(nextPage.messages || []));
      this.logger?.log(
        `[corz] messages page ${page}/${totalPages} for ticket ${ticketUuid}, received ${messages.length} items so far`
      );
    }

    this.logger?.log(`[corz] completed messages fetch for ticket ${ticketUuid}: ${messages.length} items`);

    return {
      messages,
      count: Number(firstPage.count || messages.length),
      totalPages
    };
  }
}
