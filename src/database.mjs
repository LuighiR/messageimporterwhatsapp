import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

function ensureParentDirectory(filePath) {
  const directory = path.dirname(filePath);
  fs.mkdirSync(directory, { recursive: true });
}

function toBooleanInteger(value) {
  return value ? 1 : 0;
}

function stripSessionMessages(session) {
  const { messages, ...sessionWithoutMessages } = session;
  return sessionWithoutMessages;
}

export class ImporterDatabase {
  constructor(databasePath, { logger = console } = {}) {
    ensureParentDirectory(databasePath);
    this.db = new DatabaseSync(databasePath);
    this.logger = logger;
    this.db.exec("PRAGMA foreign_keys = ON;");
    this.initSchema();
  }

  initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS tickets (
        ticket_id INTEGER PRIMARY KEY,
        ticket_uuid TEXT NOT NULL UNIQUE,
        status TEXT,
        created_at TEXT,
        updated_at TEXT,
        last_message TEXT,
        last_message_hour TEXT,
        unread_messages INTEGER,
        is_group INTEGER NOT NULL DEFAULT 0,
        from_me INTEGER NOT NULL DEFAULT 0,
        company_id INTEGER,
        tenant_id TEXT,
        queue_id INTEGER,
        queue_option_id INTEGER,
        contact_id INTEGER,
        contact_name TEXT,
        contact_number TEXT,
        contact_email TEXT,
        social_connection_id INTEGER,
        social_connection_name TEXT,
        social_connection_platform TEXT,
        raw_ticket_json TEXT NOT NULL,
        imported_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS sessions (
        session_id INTEGER PRIMARY KEY,
        ticket_id INTEGER NOT NULL,
        ticket_uuid TEXT NOT NULL,
        ticket_tracking_id INTEGER NOT NULL UNIQUE,
        sequence INTEGER NOT NULL,
        opened_at TEXT,
        started_at TEXT,
        finished_at TEXT,
        next_session_opened_at TEXT,
        assigned_user_id INTEGER,
        assigned_user_name TEXT,
        assigned_user_email TEXT,
        message_count INTEGER NOT NULL DEFAULT 0,
        raw_session_json TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS messages (
        message_id INTEGER PRIMARY KEY,
        ticket_id INTEGER NOT NULL,
        ticket_uuid TEXT NOT NULL,
        session_id INTEGER,
        body TEXT,
        media_type TEXT,
        media_url TEXT,
        from_me INTEGER NOT NULL DEFAULT 0,
        send_by_system INTEGER NOT NULL DEFAULT 0,
        created_at TEXT,
        updated_at TEXT,
        remote_jid TEXT,
        ack INTEGER,
        is_read INTEGER NOT NULL DEFAULT 0,
        quoted_msg_id INTEGER,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        message_key TEXT,
        raw_message_json TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id) ON DELETE CASCADE,
        FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE SET NULL
      );

      CREATE TABLE IF NOT EXISTS import_runs (
        import_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id INTEGER NOT NULL,
        ticket_uuid TEXT NOT NULL,
        session_count INTEGER NOT NULL,
        message_count INTEGER NOT NULL,
        imported_at TEXT NOT NULL,
        FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_sessions_ticket_uuid ON sessions(ticket_uuid);
      CREATE INDEX IF NOT EXISTS idx_messages_ticket_uuid ON messages(ticket_uuid);
      CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
      CREATE INDEX IF NOT EXISTS idx_import_runs_ticket_uuid ON import_runs(ticket_uuid);

      CREATE TABLE IF NOT EXISTS import_jobs (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL,
        start_page INTEGER NOT NULL,
        current_page INTEGER NOT NULL,
        next_ticket_offset INTEGER NOT NULL DEFAULT 0,
        current_ticket_uuid TEXT,
        limit_per_page INTEGER NOT NULL,
        pages_requested INTEGER,
        sweep_all INTEGER NOT NULL DEFAULT 0,
        max_pages INTEGER,
        persist INTEGER NOT NULL DEFAULT 1,
        start_date TEXT,
        end_date TEXT,
        total_pages INTEGER,
        pages_processed INTEGER NOT NULL DEFAULT 0,
        tickets_seen INTEGER NOT NULL DEFAULT 0,
        tickets_imported INTEGER NOT NULL DEFAULT 0,
        tickets_failed INTEGER NOT NULL DEFAULT 0,
        status_message TEXT,
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        finished_at TEXT
      );

      CREATE TABLE IF NOT EXISTS import_job_errors (
        job_error_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        ticket_id INTEGER,
        ticket_uuid TEXT,
        error_message TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (job_id) REFERENCES import_jobs(job_id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs(status);
      CREATE INDEX IF NOT EXISTS idx_import_job_errors_job_id ON import_job_errors(job_id);

      CREATE TABLE IF NOT EXISTS contacts (
        contact_id INTEGER PRIMARY KEY,
        name TEXT,
        number TEXT,
        email TEXT,
        is_group INTEGER NOT NULL DEFAULT 0,
        social_connection_id INTEGER,
        company_id INTEGER,
        created_at TEXT,
        updated_at TEXT,
        profile_pic_url TEXT,
        source TEXT NOT NULL DEFAULT 'ticket_stub',
        raw_contact_json TEXT,
        synced_at TEXT
      );

      CREATE TABLE IF NOT EXISTS tags (
        tag_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        color TEXT,
        company_id INTEGER,
        raw_tag_json TEXT
      );

      CREATE TABLE IF NOT EXISTS contact_tags (
        contact_id INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY (contact_id, tag_id),
        FOREIGN KEY (contact_id) REFERENCES contacts(contact_id) ON DELETE CASCADE,
        FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS contact_extra_info (
        extra_info_id INTEGER PRIMARY KEY,
        contact_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        value TEXT,
        FOREIGN KEY (contact_id) REFERENCES contacts(contact_id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS contact_sync_jobs (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL,
        start_page INTEGER NOT NULL,
        current_page INTEGER NOT NULL,
        limit_per_page INTEGER NOT NULL,
        pages_requested INTEGER,
        sweep_all INTEGER NOT NULL DEFAULT 0,
        max_pages INTEGER,
        total_pages INTEGER,
        pages_processed INTEGER NOT NULL DEFAULT 0,
        contacts_seen INTEGER NOT NULL DEFAULT 0,
        contacts_synced INTEGER NOT NULL DEFAULT 0,
        contacts_failed INTEGER NOT NULL DEFAULT 0,
        status_message TEXT,
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        finished_at TEXT
      );

      CREATE TABLE IF NOT EXISTS contact_sync_job_errors (
        job_error_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        contact_id INTEGER,
        error_message TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (job_id) REFERENCES contact_sync_jobs(job_id) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_contact_tags_contact_id ON contact_tags(contact_id);
      CREATE INDEX IF NOT EXISTS idx_contact_extra_info_contact_id ON contact_extra_info(contact_id);
      CREATE INDEX IF NOT EXISTS idx_contact_sync_jobs_status ON contact_sync_jobs(status);
      CREATE INDEX IF NOT EXISTS idx_contact_sync_job_errors_job_id ON contact_sync_job_errors(job_id);
    `);

    this.ensureColumn("import_jobs", "start_date", "TEXT");
    this.ensureColumn("import_jobs", "end_date", "TEXT");
  }

  ensureColumn(tableName, columnName, columnDefinition) {
    const columns = this.db.prepare(`PRAGMA table_info(${tableName})`).all();
    const exists = columns.some((column) => column.name === columnName);

    if (!exists) {
      this.db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDefinition}`);
    }
  }

  persistSnapshot(snapshot) {
    const importedAt = new Date().toISOString();
    const { ticket } = snapshot;
    this.ensureContactStub({
      ...ticket.contact,
      companyId: ticket.companyId,
      socialConnectionId: ticket.socialConnection?.id || null
    });
    this.logger?.log(
      `[sqlite] persisting ticket ${ticket.uuid} (${ticket.id}) with ${snapshot.sessionCount} sessions and ${snapshot.messageCount} messages`
    );

    this.db.exec("BEGIN");

    try {
      this.db.prepare(`
        INSERT INTO tickets (
          ticket_id, ticket_uuid, status, created_at, updated_at, last_message, last_message_hour,
          unread_messages, is_group, from_me, company_id, tenant_id, queue_id, queue_option_id,
          contact_id, contact_name, contact_number, contact_email,
          social_connection_id, social_connection_name, social_connection_platform,
          raw_ticket_json, imported_at
        ) VALUES (
          @ticket_id, @ticket_uuid, @status, @created_at, @updated_at, @last_message, @last_message_hour,
          @unread_messages, @is_group, @from_me, @company_id, @tenant_id, @queue_id, @queue_option_id,
          @contact_id, @contact_name, @contact_number, @contact_email,
          @social_connection_id, @social_connection_name, @social_connection_platform,
          @raw_ticket_json, @imported_at
        )
        ON CONFLICT(ticket_id) DO UPDATE SET
          ticket_uuid = excluded.ticket_uuid,
          status = excluded.status,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at,
          last_message = excluded.last_message,
          last_message_hour = excluded.last_message_hour,
          unread_messages = excluded.unread_messages,
          is_group = excluded.is_group,
          from_me = excluded.from_me,
          company_id = excluded.company_id,
          tenant_id = excluded.tenant_id,
          queue_id = excluded.queue_id,
          queue_option_id = excluded.queue_option_id,
          contact_id = excluded.contact_id,
          contact_name = excluded.contact_name,
          contact_number = excluded.contact_number,
          contact_email = excluded.contact_email,
          social_connection_id = excluded.social_connection_id,
          social_connection_name = excluded.social_connection_name,
          social_connection_platform = excluded.social_connection_platform,
          raw_ticket_json = excluded.raw_ticket_json,
          imported_at = excluded.imported_at
      `).run({
        ticket_id: ticket.id,
        ticket_uuid: ticket.uuid,
        status: ticket.status,
        created_at: ticket.createdAt,
        updated_at: ticket.updatedAt,
        last_message: ticket.lastMessage,
        last_message_hour: ticket.lastMessageHour,
        unread_messages: ticket.unreadMessages,
        is_group: toBooleanInteger(ticket.isGroup),
        from_me: toBooleanInteger(ticket.fromMe),
        company_id: ticket.companyId,
        tenant_id: ticket.tenantId,
        queue_id: ticket.queueId,
        queue_option_id: ticket.queueOptionId,
        contact_id: ticket.contact?.id || null,
        contact_name: ticket.contact?.name || null,
        contact_number: ticket.contact?.number || null,
        contact_email: ticket.contact?.email || null,
        social_connection_id: ticket.socialConnection?.id || null,
        social_connection_name: ticket.socialConnection?.name || null,
        social_connection_platform: ticket.socialConnection?.platform || null,
        raw_ticket_json: JSON.stringify(ticket),
        imported_at: importedAt
      });

      this.db.prepare("DELETE FROM messages WHERE ticket_id = ?").run(ticket.id);
      this.db.prepare("DELETE FROM sessions WHERE ticket_id = ?").run(ticket.id);

      const insertSession = this.db.prepare(`
        INSERT INTO sessions (
          session_id, ticket_id, ticket_uuid, ticket_tracking_id, sequence, opened_at, started_at,
          finished_at, next_session_opened_at, assigned_user_id, assigned_user_name, assigned_user_email,
          message_count, raw_session_json, imported_at
        ) VALUES (
          @session_id, @ticket_id, @ticket_uuid, @ticket_tracking_id, @sequence, @opened_at, @started_at,
          @finished_at, @next_session_opened_at, @assigned_user_id, @assigned_user_name, @assigned_user_email,
          @message_count, @raw_session_json, @imported_at
        )
      `);

      const insertMessage = this.db.prepare(`
        INSERT INTO messages (
          message_id, ticket_id, ticket_uuid, session_id, body, media_type, media_url, from_me,
          send_by_system, created_at, updated_at, remote_jid, ack, is_read, quoted_msg_id,
          is_deleted, message_key, raw_message_json, imported_at
        ) VALUES (
          @message_id, @ticket_id, @ticket_uuid, @session_id, @body, @media_type, @media_url, @from_me,
          @send_by_system, @created_at, @updated_at, @remote_jid, @ack, @is_read, @quoted_msg_id,
          @is_deleted, @message_key, @raw_message_json, @imported_at
        )
      `);

      for (const session of snapshot.sessions) {
        insertSession.run({
          session_id: session.sessionId,
          ticket_id: session.ticketId,
          ticket_uuid: session.ticketUuid,
          ticket_tracking_id: session.ticketTrackingId,
          sequence: session.sequence,
          opened_at: session.openedAt,
          started_at: session.startedAt,
          finished_at: session.finishedAt,
          next_session_opened_at: session.nextSessionOpenedAt,
          assigned_user_id: session.assignedUser?.id || null,
          assigned_user_name: session.assignedUser?.name || null,
          assigned_user_email: session.assignedUser?.email || null,
          message_count: session.messageCount,
          raw_session_json: JSON.stringify(stripSessionMessages(session)),
          imported_at: importedAt
        });

        for (const message of session.messages) {
          insertMessage.run({
            message_id: message.id,
            ticket_id: message.ticketId,
            ticket_uuid: session.ticketUuid,
            session_id: session.sessionId,
            body: message.body,
            media_type: message.mediaType,
            media_url: message.mediaUrl,
            from_me: toBooleanInteger(message.fromMe),
            send_by_system: toBooleanInteger(message.sendBySystem),
            created_at: message.createdAt,
            updated_at: message.updatedAt,
            remote_jid: message.remoteJid,
            ack: message.ack,
            is_read: toBooleanInteger(message.read),
            quoted_msg_id: message.quotedMsgId,
            is_deleted: toBooleanInteger(message.isDeleted),
            message_key: message.key,
            raw_message_json: JSON.stringify(message),
            imported_at: importedAt
          });
        }
      }

      for (const message of snapshot.unassignedMessages || []) {
        insertMessage.run({
          message_id: message.id,
          ticket_id: message.ticketId,
          ticket_uuid: ticket.uuid,
          session_id: null,
          body: message.body,
          media_type: message.mediaType,
          media_url: message.mediaUrl,
          from_me: toBooleanInteger(message.fromMe),
          send_by_system: toBooleanInteger(message.sendBySystem),
          created_at: message.createdAt,
          updated_at: message.updatedAt,
          remote_jid: message.remoteJid,
          ack: message.ack,
          is_read: toBooleanInteger(message.read),
          quoted_msg_id: message.quotedMsgId,
          is_deleted: toBooleanInteger(message.isDeleted),
          message_key: message.key,
          raw_message_json: JSON.stringify(message),
          imported_at: importedAt
        });
      }

      this.db.prepare(`
        INSERT INTO import_runs (
          ticket_id, ticket_uuid, session_count, message_count, imported_at
        ) VALUES (?, ?, ?, ?, ?)
      `).run(ticket.id, ticket.uuid, snapshot.sessionCount, snapshot.messageCount, importedAt);

      this.db.exec("COMMIT");
      this.logger?.log(`[sqlite] persisted ticket ${ticket.uuid} at ${importedAt}`);

      return {
        importedAt,
        ticketId: ticket.id,
        ticketUuid: ticket.uuid,
        sessionCount: snapshot.sessionCount,
        messageCount: snapshot.messageCount
      };
    } catch (error) {
      this.db.exec("ROLLBACK");
      this.logger?.error?.(`[sqlite] rollback while persisting ticket ${ticket.uuid}: ${error.message}`);
      throw error;
    }
  }

  listTickets(limit = 50) {
    const rows = this.db.prepare(`
      SELECT ticket_id, ticket_uuid, status, imported_at, updated_at, unread_messages
      FROM tickets
      ORDER BY imported_at DESC
      LIMIT ?
    `).all(limit);

    return rows.map((row) => ({
      ticketId: row.ticket_id,
      ticketUuid: row.ticket_uuid,
      status: row.status,
      importedAt: row.imported_at,
      updatedAt: row.updated_at,
      unreadMessages: row.unread_messages
    }));
  }

  getTicketSnapshot(ticketUuid) {
    const ticketRow = this.db.prepare(`
      SELECT raw_ticket_json
      FROM tickets
      WHERE ticket_uuid = ?
    `).get(ticketUuid);

    if (!ticketRow) {
      return null;
    }

    const sessions = this.db.prepare(`
      SELECT session_id, raw_session_json
      FROM sessions
      WHERE ticket_uuid = ?
      ORDER BY sequence ASC
    `).all(ticketUuid);

    const messages = this.db.prepare(`
      SELECT session_id, raw_message_json
      FROM messages
      WHERE ticket_uuid = ?
      ORDER BY datetime(created_at) ASC, message_id ASC
    `).all(ticketUuid);

    const importRun = this.db.prepare(`
      SELECT imported_at, session_count, message_count
      FROM import_runs
      WHERE ticket_uuid = ?
      ORDER BY import_run_id DESC
      LIMIT 1
    `).get(ticketUuid);

    const parsedSessions = sessions.map((row) => ({
      ...JSON.parse(row.raw_session_json),
      messages: [],
      messageCount: 0
    }));
    const sessionMap = new Map(parsedSessions.map((session) => [session.sessionId, session]));
    const unassignedMessages = [];

    for (const row of messages) {
      const message = JSON.parse(row.raw_message_json);
      if (row.session_id && sessionMap.has(row.session_id)) {
        sessionMap.get(row.session_id).messages.push(message);
      } else {
        unassignedMessages.push(message);
      }
    }

    for (const session of parsedSessions) {
      session.messageCount = session.messages.length;
    }

    return {
      ticket: JSON.parse(ticketRow.raw_ticket_json),
      sessionCount: parsedSessions.length,
      messageCount: importRun?.message_count || messages.length,
      sessionAssignmentRule:
        "A message belongs to the latest ticketTracking whose createdAt is <= message.createdAt and before the next ticketTracking createdAt.",
      importedAt: importRun?.imported_at || null,
      sessions: parsedSessions,
      unassignedMessages
    };
  }

  createImportJob({ page, limit, pages, sweepAll, maxPages, persist, startDate = null, endDate = null }) {
    const now = new Date().toISOString();
    const result = this.db.prepare(`
      INSERT INTO import_jobs (
        status, start_page, current_page, next_ticket_offset, current_ticket_uuid,
        limit_per_page, pages_requested, sweep_all, max_pages, persist, start_date, end_date,
        total_pages, pages_processed, tickets_seen, tickets_imported, tickets_failed,
        status_message, started_at, updated_at, finished_at
      ) VALUES (
        @status, @start_page, @current_page, @next_ticket_offset, @current_ticket_uuid,
        @limit_per_page, @pages_requested, @sweep_all, @max_pages, @persist, @start_date, @end_date,
        @total_pages, @pages_processed, @tickets_seen, @tickets_imported, @tickets_failed,
        @status_message, @started_at, @updated_at, @finished_at
      )
    `).run({
      status: "pending",
      start_page: page,
      current_page: page,
      next_ticket_offset: 0,
      current_ticket_uuid: null,
      limit_per_page: limit,
      pages_requested: sweepAll ? null : pages,
      sweep_all: toBooleanInteger(sweepAll),
      max_pages: maxPages,
      persist: toBooleanInteger(persist),
      start_date: startDate,
      end_date: endDate,
      total_pages: null,
      pages_processed: 0,
      tickets_seen: 0,
      tickets_imported: 0,
      tickets_failed: 0,
      status_message: "Job created",
      started_at: now,
      updated_at: now,
      finished_at: null
    });

    return this.getImportJob(result.lastInsertRowid);
  }

  getImportJob(jobId) {
    const row = this.db.prepare(`
      SELECT *
      FROM import_jobs
      WHERE job_id = ?
    `).get(jobId);

    return row ? this.mapImportJob(row) : null;
  }

  listImportJobs(limit = 20) {
    const rows = this.db.prepare(`
      SELECT *
      FROM import_jobs
      ORDER BY job_id DESC
      LIMIT ?
    `).all(limit);

    return rows.map((row) => this.mapImportJob(row));
  }

  getRunningImportJob() {
    const row = this.db.prepare(`
      SELECT *
      FROM import_jobs
      WHERE status = 'running'
      ORDER BY job_id DESC
      LIMIT 1
    `).get();

    return row ? this.mapImportJob(row) : null;
  }

  updateImportJob(jobId, fields) {
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined);
    if (entries.length === 0) {
      return this.getImportJob(jobId);
    }

    const now = new Date().toISOString();
    const assignments = entries.map(([key]) => `${key} = @${key}`);
    assignments.push("updated_at = @updated_at");

    const params = Object.fromEntries(entries);
    params.updated_at = now;
    params.job_id = jobId;

    this.db.prepare(`
      UPDATE import_jobs
      SET ${assignments.join(", ")}
      WHERE job_id = @job_id
    `).run(params);

    return this.getImportJob(jobId);
  }

  addImportJobError(jobId, { ticketId = null, ticketUuid = null, errorMessage }) {
    this.db.prepare(`
      INSERT INTO import_job_errors (
        job_id, ticket_id, ticket_uuid, error_message, created_at
      ) VALUES (?, ?, ?, ?, ?)
    `).run(jobId, ticketId, ticketUuid, errorMessage, new Date().toISOString());
  }

  listImportJobErrors(jobId, limit = 100) {
    return this.db.prepare(`
      SELECT job_error_id, ticket_id, ticket_uuid, error_message, created_at
      FROM import_job_errors
      WHERE job_id = ?
      ORDER BY job_error_id DESC
      LIMIT ?
    `).all(jobId, limit).map((row) => ({
      jobErrorId: row.job_error_id,
      ticketId: row.ticket_id,
      ticketUuid: row.ticket_uuid,
      errorMessage: row.error_message,
      createdAt: row.created_at
    }));
  }

  mapImportJob(row) {
    return {
      jobId: row.job_id,
      status: row.status,
      startPage: row.start_page,
      currentPage: row.current_page,
      nextTicketOffset: row.next_ticket_offset,
      currentTicketUuid: row.current_ticket_uuid,
      limitPerPage: row.limit_per_page,
      pagesRequested: row.pages_requested,
      sweepAll: Boolean(row.sweep_all),
      maxPages: row.max_pages,
      persist: Boolean(row.persist),
      startDate: row.start_date,
      endDate: row.end_date,
      totalPages: row.total_pages,
      pagesProcessed: row.pages_processed,
      ticketsSeen: row.tickets_seen,
      ticketsImported: row.tickets_imported,
      ticketsFailed: row.tickets_failed,
      statusMessage: row.status_message,
      startedAt: row.started_at,
      updatedAt: row.updated_at,
      finishedAt: row.finished_at
    };
  }

  ensureContactStub(contact) {
    if (!contact?.id) {
      return;
    }

    this.db.prepare(`
      INSERT OR IGNORE INTO contacts (
        contact_id, name, number, email, is_group, social_connection_id, company_id,
        created_at, updated_at, profile_pic_url, source, raw_contact_json, synced_at
      ) VALUES (
        @contact_id, @name, @number, @email, @is_group, @social_connection_id, @company_id,
        @created_at, @updated_at, @profile_pic_url, @source, @raw_contact_json, @synced_at
      )
    `).run({
      contact_id: contact.id,
      name: contact.name || null,
      number: contact.number || null,
      email: contact.email || null,
      is_group: 0,
      social_connection_id: contact.socialConnectionId || null,
      company_id: contact.companyId || null,
      created_at: null,
      updated_at: null,
      profile_pic_url: contact.profilePicUrl || null,
      source: "ticket_stub",
      raw_contact_json: JSON.stringify(contact),
      synced_at: null
    });
  }

  upsertContact(contact) {
    const syncedAt = new Date().toISOString();

    this.db.exec("BEGIN");

    try {
      this.db.prepare(`
        INSERT INTO contacts (
          contact_id, name, number, email, is_group, social_connection_id, company_id,
          created_at, updated_at, profile_pic_url, source, raw_contact_json, synced_at
        ) VALUES (
          @contact_id, @name, @number, @email, @is_group, @social_connection_id, @company_id,
          @created_at, @updated_at, @profile_pic_url, @source, @raw_contact_json, @synced_at
        )
        ON CONFLICT(contact_id) DO UPDATE SET
          name = excluded.name,
          number = excluded.number,
          email = excluded.email,
          is_group = excluded.is_group,
          social_connection_id = excluded.social_connection_id,
          company_id = excluded.company_id,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at,
          profile_pic_url = excluded.profile_pic_url,
          source = excluded.source,
          raw_contact_json = excluded.raw_contact_json,
          synced_at = excluded.synced_at
      `).run({
        contact_id: contact.id,
        name: contact.name || null,
        number: contact.number || null,
        email: contact.email || null,
        is_group: toBooleanInteger(contact.isGroup),
        social_connection_id: contact.socialConnectionId || null,
        company_id: contact.companyId || null,
        created_at: contact.createdAt || null,
        updated_at: contact.updatedAt || null,
        profile_pic_url: contact.profilePicUrl || null,
        source: "contact_sync",
        raw_contact_json: JSON.stringify(contact),
        synced_at: syncedAt
      });

      this.db.prepare("DELETE FROM contact_tags WHERE contact_id = ?").run(contact.id);
      this.db.prepare("DELETE FROM contact_extra_info WHERE contact_id = ?").run(contact.id);

      const insertTag = this.db.prepare(`
        INSERT INTO tags (
          tag_id, name, color, company_id, raw_tag_json
        ) VALUES (
          @tag_id, @name, @color, @company_id, @raw_tag_json
        )
        ON CONFLICT(tag_id) DO UPDATE SET
          name = excluded.name,
          color = excluded.color,
          company_id = excluded.company_id,
          raw_tag_json = excluded.raw_tag_json
      `);

      const insertContactTag = this.db.prepare(`
        INSERT OR IGNORE INTO contact_tags (contact_id, tag_id)
        VALUES (?, ?)
      `);

      for (const tag of contact.tags || []) {
        insertTag.run({
          tag_id: tag.id,
          name: tag.name,
          color: tag.color || null,
          company_id: tag.companyId || null,
          raw_tag_json: JSON.stringify(tag)
        });
        insertContactTag.run(contact.id, tag.id);
      }

      const insertExtraInfo = this.db.prepare(`
        INSERT INTO contact_extra_info (
          extra_info_id, contact_id, name, value
        ) VALUES (?, ?, ?, ?)
      `);

      for (const item of contact.extraInfo || []) {
        insertExtraInfo.run(item.id, contact.id, item.name, item.value || null);
      }

      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  listContacts(limit = 50) {
    const rows = this.db.prepare(`
      SELECT contact_id, name, number, email, source, synced_at, updated_at
      FROM contacts
      ORDER BY COALESCE(synced_at, updated_at) DESC, contact_id DESC
      LIMIT ?
    `).all(limit);

    return rows.map((row) => ({
      contactId: row.contact_id,
      name: row.name,
      number: row.number,
      email: row.email,
      source: row.source,
      syncedAt: row.synced_at,
      updatedAt: row.updated_at
    }));
  }

  getContact(contactId) {
    const row = this.db.prepare(`
      SELECT *
      FROM contacts
      WHERE contact_id = ?
    `).get(contactId);

    if (!row) {
      return null;
    }

    const tags = this.db.prepare(`
      SELECT t.tag_id, t.name, t.color, t.company_id
      FROM tags t
      INNER JOIN contact_tags ct ON ct.tag_id = t.tag_id
      WHERE ct.contact_id = ?
      ORDER BY t.name ASC
    `).all(contactId).map((tag) => ({
      id: tag.tag_id,
      name: tag.name,
      color: tag.color,
      companyId: tag.company_id
    }));

    const extraInfo = this.db.prepare(`
      SELECT extra_info_id, name, value
      FROM contact_extra_info
      WHERE contact_id = ?
      ORDER BY extra_info_id ASC
    `).all(contactId).map((item) => ({
      id: item.extra_info_id,
      name: item.name,
      value: item.value
    }));

    return {
      contactId: row.contact_id,
      name: row.name,
      number: row.number,
      email: row.email,
      isGroup: Boolean(row.is_group),
      socialConnectionId: row.social_connection_id,
      companyId: row.company_id,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      profilePicUrl: row.profile_pic_url,
      source: row.source,
      syncedAt: row.synced_at,
      tags,
      extraInfo
    };
  }

  createContactSyncJob({ page, limit, pages, sweepAll, maxPages }) {
    const now = new Date().toISOString();
    const result = this.db.prepare(`
      INSERT INTO contact_sync_jobs (
        status, start_page, current_page, limit_per_page, pages_requested, sweep_all, max_pages,
        total_pages, pages_processed, contacts_seen, contacts_synced, contacts_failed,
        status_message, started_at, updated_at, finished_at
      ) VALUES (
        @status, @start_page, @current_page, @limit_per_page, @pages_requested, @sweep_all, @max_pages,
        @total_pages, @pages_processed, @contacts_seen, @contacts_synced, @contacts_failed,
        @status_message, @started_at, @updated_at, @finished_at
      )
    `).run({
      status: "pending",
      start_page: page,
      current_page: page,
      limit_per_page: limit,
      pages_requested: sweepAll ? null : pages,
      sweep_all: toBooleanInteger(sweepAll),
      max_pages: maxPages,
      total_pages: null,
      pages_processed: 0,
      contacts_seen: 0,
      contacts_synced: 0,
      contacts_failed: 0,
      status_message: "Contact sync job created",
      started_at: now,
      updated_at: now,
      finished_at: null
    });

    return this.getContactSyncJob(result.lastInsertRowid);
  }

  getContactSyncJob(jobId) {
    const row = this.db.prepare(`
      SELECT *
      FROM contact_sync_jobs
      WHERE job_id = ?
    `).get(jobId);

    return row ? this.mapContactSyncJob(row) : null;
  }

  listContactSyncJobs(limit = 20) {
    const rows = this.db.prepare(`
      SELECT *
      FROM contact_sync_jobs
      ORDER BY job_id DESC
      LIMIT ?
    `).all(limit);

    return rows.map((row) => this.mapContactSyncJob(row));
  }

  getRunningContactSyncJob() {
    const row = this.db.prepare(`
      SELECT *
      FROM contact_sync_jobs
      WHERE status = 'running'
      ORDER BY job_id DESC
      LIMIT 1
    `).get();

    return row ? this.mapContactSyncJob(row) : null;
  }

  updateContactSyncJob(jobId, fields) {
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined);
    if (entries.length === 0) {
      return this.getContactSyncJob(jobId);
    }

    const now = new Date().toISOString();
    const assignments = entries.map(([key]) => `${key} = @${key}`);
    assignments.push("updated_at = @updated_at");

    const params = Object.fromEntries(entries);
    params.updated_at = now;
    params.job_id = jobId;

    this.db.prepare(`
      UPDATE contact_sync_jobs
      SET ${assignments.join(", ")}
      WHERE job_id = @job_id
    `).run(params);

    return this.getContactSyncJob(jobId);
  }

  addContactSyncJobError(jobId, { contactId = null, errorMessage }) {
    this.db.prepare(`
      INSERT INTO contact_sync_job_errors (
        job_id, contact_id, error_message, created_at
      ) VALUES (?, ?, ?, ?)
    `).run(jobId, contactId, errorMessage, new Date().toISOString());
  }

  listContactSyncJobErrors(jobId, limit = 100) {
    return this.db.prepare(`
      SELECT job_error_id, contact_id, error_message, created_at
      FROM contact_sync_job_errors
      WHERE job_id = ?
      ORDER BY job_error_id DESC
      LIMIT ?
    `).all(jobId, limit).map((row) => ({
      jobErrorId: row.job_error_id,
      contactId: row.contact_id,
      errorMessage: row.error_message,
      createdAt: row.created_at
    }));
  }

  mapContactSyncJob(row) {
    return {
      jobId: row.job_id,
      status: row.status,
      startPage: row.start_page,
      currentPage: row.current_page,
      limitPerPage: row.limit_per_page,
      pagesRequested: row.pages_requested,
      sweepAll: Boolean(row.sweep_all),
      maxPages: row.max_pages,
      totalPages: row.total_pages,
      pagesProcessed: row.pages_processed,
      contactsSeen: row.contacts_seen,
      contactsSynced: row.contacts_synced,
      contactsFailed: row.contacts_failed,
      statusMessage: row.status_message,
      startedAt: row.started_at,
      updatedAt: row.updated_at,
      finishedAt: row.finished_at
    };
  }
}
