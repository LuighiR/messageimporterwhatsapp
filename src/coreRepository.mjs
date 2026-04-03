import crypto from "node:crypto";
import pg from "pg";

function nowIso() {
  return new Date().toISOString();
}

function sessionTypeForTracking(tracking) {
  if (tracking?.finishedAt) {
    return "CLOSED";
  }

  if (tracking?.startedAt) {
    return "OPEN_REAL";
  }

  return "OPEN_WEAK";
}

function senderTypeForMessage(message) {
  if (message.generatedByAi) {
    return "AI";
  }

  if (message.sendBySystem) {
    return "SYSTEM";
  }

  return "HUMAN";
}

function maxMessageCreatedAt(messages) {
  const timestamps = messages
    .map((message) => message.createdAt)
    .filter(Boolean)
    .sort((left, right) => new Date(right).getTime() - new Date(left).getTime());

  return timestamps[0] || null;
}

function makeTicketId(clientId, ticket) {
  return `ticket:${clientId}:${ticket.uuid}`;
}

function makeImportedTrackingId(clientId, ticket, tracking) {
  return `tracking:${clientId}:${ticket.uuid}:${tracking.id}`;
}

function makeSessionId(clientId, ticket, tracking) {
  return `session:${clientId}:${ticket.uuid}:${tracking.id}`;
}

function makeMessageId(clientId, ticket, message) {
  return `message:${clientId}:${ticket.uuid}:${message.id}`;
}

export class CoreRepository {
  constructor({ connectionString, schema = "core", logger = console }) {
    this.schema = schema;
    this.logger = logger;
    this.pool = new pg.Pool({
      connectionString
    });
  }

  async init() {
    await this.pool.query(`
      ALTER TABLE ${this.schema}.import_jobs
      ADD COLUMN IF NOT EXISTS client_id text
    `);
    await this.pool.query(`
      ALTER TABLE ${this.schema}.contact_sync_jobs
      ADD COLUMN IF NOT EXISTS client_id text
    `);
    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_import_jobs_client_id
      ON ${this.schema}.import_jobs (client_id)
    `);
    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_contact_sync_jobs_client_id
      ON ${this.schema}.contact_sync_jobs (client_id)
    `);
  }

  async listActiveClients() {
    const result = await this.pool.query(`
      SELECT id, slug, name, domain_uuid, api_base_url, api_key, is_active, created_at, updated_at
      FROM ${this.schema}.sinapse_clients
      WHERE is_active = true
      ORDER BY created_at ASC
    `);

    return result.rows;
  }

  async getClient(clientId) {
    const result = await this.pool.query(
      `
        SELECT id, slug, name, domain_uuid, api_base_url, api_key, is_active, created_at, updated_at
        FROM ${this.schema}.sinapse_clients
        WHERE id = $1
      `,
      [clientId]
    );

    return result.rows[0] || null;
  }

  async persistTicketSnapshot({ clientId, snapshot }) {
    const client = await this.pool.connect();
    const importedAt = nowIso();

    try {
      await client.query("BEGIN");

      await this.ensureContactStub(client, { clientId, ticket: snapshot.ticket });

      const ticketId = makeTicketId(clientId, snapshot.ticket);
      const lastImportedMessageCreatedAt = maxMessageCreatedAt(
        snapshot.sessions.flatMap((session) => session.messages).concat(snapshot.unassignedMessages || [])
      );

      await client.query(
        `
          INSERT INTO ${this.schema}.tickets (
            id, external_uuid, external_ticket_id, status, contact_name, contact_number, contact_external_id,
            social_connection_id, company_id, created_at_external, updated_at_external,
            last_imported_message_created_at, created_at, updated_at, client_id, contact_id, is_group
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17
          )
          ON CONFLICT (client_id, external_uuid) DO UPDATE SET
            external_ticket_id = EXCLUDED.external_ticket_id,
            status = EXCLUDED.status,
            contact_name = EXCLUDED.contact_name,
            contact_number = EXCLUDED.contact_number,
            contact_external_id = EXCLUDED.contact_external_id,
            social_connection_id = EXCLUDED.social_connection_id,
            company_id = EXCLUDED.company_id,
            created_at_external = EXCLUDED.created_at_external,
            updated_at_external = EXCLUDED.updated_at_external,
            last_imported_message_created_at = EXCLUDED.last_imported_message_created_at,
            updated_at = EXCLUDED.updated_at,
            contact_id = EXCLUDED.contact_id,
            is_group = EXCLUDED.is_group
        `,
        [
          ticketId,
          snapshot.ticket.uuid,
          snapshot.ticket.id,
          snapshot.ticket.status,
          snapshot.ticket.contact?.name || null,
          snapshot.ticket.contact?.number || null,
          snapshot.ticket.contact?.id || null,
          snapshot.ticket.socialConnection?.id || null,
          snapshot.ticket.companyId || null,
          snapshot.ticket.createdAt || null,
          snapshot.ticket.updatedAt || null,
          lastImportedMessageCreatedAt,
          importedAt,
          importedAt,
          clientId,
          snapshot.ticket.contact?.id || null,
          Boolean(snapshot.ticket.isGroup)
        ]
      );

      await client.query(`DELETE FROM ${this.schema}.messages WHERE ticket_id = $1`, [ticketId]);
      await client.query(`DELETE FROM ${this.schema}.sessions WHERE ticket_id = $1`, [ticketId]);
      await client.query(`DELETE FROM ${this.schema}.imported_trackings WHERE ticket_id = $1`, [ticketId]);

      for (const session of snapshot.sessions) {
        const importedTrackingId = makeImportedTrackingId(clientId, snapshot.ticket, {
          id: session.ticketTrackingId
        });
        const sessionId = makeSessionId(clientId, snapshot.ticket, {
          id: session.ticketTrackingId
        });

        await client.query(
          `
            INSERT INTO ${this.schema}.imported_trackings (
              id, ticket_id, external_tracking_id, created_at_external, started_at_external,
              ended_at_external, last_rebuild_message_created_at, processed_at, processing_version,
              processing_notes, created_at, updated_at
            ) VALUES (
              $1, $2, $3, $4, $5,
              $6, $7, $8, $9,
              $10, $11, $12
            )
            ON CONFLICT (id) DO UPDATE SET
              started_at_external = EXCLUDED.started_at_external,
              ended_at_external = EXCLUDED.ended_at_external,
              last_rebuild_message_created_at = EXCLUDED.last_rebuild_message_created_at,
              processed_at = EXCLUDED.processed_at,
              processing_version = EXCLUDED.processing_version,
              processing_notes = EXCLUDED.processing_notes,
              updated_at = EXCLUDED.updated_at
          `,
          [
            importedTrackingId,
            ticketId,
            session.ticketTrackingId,
            session.openedAt || null,
            session.startedAt || null,
            session.finishedAt || null,
            maxMessageCreatedAt(session.messages),
            importedAt,
            "v1",
            null,
            importedAt,
            importedAt
          ]
        );

        await client.query(
          `
            INSERT INTO ${this.schema}.sessions (
              id, ticket_id, external_tracking_id, type, started_at, ended_at,
              assigned_user_name, assigned_user_email, created_at, updated_at,
              origin_imported_tracking_id, processing_version, source, created_at_external
            ) VALUES (
              $1, $2, $3, $4, $5, $6,
              $7, $8, $9, $10,
              $11, $12, $13, $14
            )
            ON CONFLICT (ticket_id, external_tracking_id) DO UPDATE SET
              type = EXCLUDED.type,
              started_at = EXCLUDED.started_at,
              ended_at = EXCLUDED.ended_at,
              assigned_user_name = EXCLUDED.assigned_user_name,
              assigned_user_email = EXCLUDED.assigned_user_email,
              updated_at = EXCLUDED.updated_at,
              origin_imported_tracking_id = EXCLUDED.origin_imported_tracking_id,
              processing_version = EXCLUDED.processing_version,
              source = EXCLUDED.source,
              created_at_external = EXCLUDED.created_at_external
          `,
          [
            sessionId,
            ticketId,
            session.ticketTrackingId,
            sessionTypeForTracking({
              startedAt: session.startedAt,
              finishedAt: session.finishedAt
            }),
            session.startedAt || session.openedAt || importedAt,
            session.finishedAt || null,
            session.assignedUser?.name || null,
            session.assignedUser?.email || null,
            importedAt,
            importedAt,
            importedTrackingId,
            "v1",
            "corz_import",
            session.openedAt || null
          ]
        );

        for (const message of session.messages) {
          await this.insertMessage(client, {
            clientId,
            ticket: snapshot.ticket,
            ticketId,
            sessionId,
            message,
            importedAt
          });
        }
      }

      for (const message of snapshot.unassignedMessages || []) {
        await this.insertMessage(client, {
          clientId,
          ticket: snapshot.ticket,
          ticketId,
          sessionId: null,
          message,
          importedAt
        });
      }

      await client.query(
        `
          INSERT INTO ${this.schema}.import_runs (
            ticket_id, ticket_uuid, session_count, message_count, imported_at
          ) VALUES ($1, $2, $3, $4, $5)
        `,
        [ticketId, snapshot.ticket.uuid, snapshot.sessionCount, snapshot.messageCount, importedAt]
      );

      await client.query("COMMIT");

      return {
        importedAt,
        ticketId,
        ticketUuid: snapshot.ticket.uuid,
        sessionCount: snapshot.sessionCount,
        messageCount: snapshot.messageCount
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async ensureContactStub(clientConnection, { clientId, ticket }) {
    const contact = ticket.contact;
    if (!contact?.id) {
      return;
    }

    await clientConnection.query(
      `
        INSERT INTO ${this.schema}.contacts (
          id, client_id, company_id, name, number, email, is_group,
          social_connection_id, profile_pic_url, created_at_remote, updated_at_remote,
          created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7,
          $8, $9, $10, $11,
          $12, $13
        )
        ON CONFLICT (client_id, id) DO NOTHING
      `,
      [
        contact.id,
        clientId,
        ticket.companyId || null,
        contact.name || null,
        contact.number || null,
        contact.email || null,
        Boolean(ticket.isGroup),
        ticket.socialConnection?.id || null,
        contact.profilePicUrl || null,
        null,
        null,
        nowIso(),
        nowIso()
      ]
    );
  }

  async insertMessage(clientConnection, { clientId, ticket, ticketId, sessionId, message, importedAt }) {
    const messageId = makeMessageId(clientId, ticket, message);
    await clientConnection.query(
      `
        INSERT INTO ${this.schema}.messages (
          id, ticket_id, session_id, external_message_id, key, body, from_me, media_url,
          media_type, created_at_external, updated_at_external, raw_json, created_at, updated_at, sender_type
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12::jsonb, $13, $14, $15::${this.schema}.message_sender_type
        )
        ON CONFLICT (ticket_id, external_message_id) DO UPDATE SET
          session_id = EXCLUDED.session_id,
          key = EXCLUDED.key,
          body = EXCLUDED.body,
          from_me = EXCLUDED.from_me,
          media_url = EXCLUDED.media_url,
          media_type = EXCLUDED.media_type,
          created_at_external = EXCLUDED.created_at_external,
          updated_at_external = EXCLUDED.updated_at_external,
          raw_json = EXCLUDED.raw_json,
          updated_at = EXCLUDED.updated_at,
          sender_type = EXCLUDED.sender_type
      `,
      [
        messageId,
        ticketId,
        sessionId,
        String(message.id),
        message.key || null,
        message.body || "",
        Boolean(message.fromMe),
        message.mediaUrl || null,
        message.mediaType || null,
        message.createdAt,
        message.updatedAt || message.createdAt,
        JSON.stringify(message),
        importedAt,
        importedAt,
        senderTypeForMessage(message)
      ]
    );
  }

  async upsertContact(clientId, contact) {
    const dbClient = await this.pool.connect();
    const timestamp = nowIso();

    try {
      await dbClient.query("BEGIN");

      await dbClient.query(
        `
          INSERT INTO ${this.schema}.contacts (
            id, client_id, company_id, name, number, email, is_group,
            social_connection_id, profile_pic_url, created_at_remote, updated_at_remote,
            created_at, updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13
          )
          ON CONFLICT (client_id, id) DO UPDATE SET
            company_id = EXCLUDED.company_id,
            name = EXCLUDED.name,
            number = EXCLUDED.number,
            email = EXCLUDED.email,
            is_group = EXCLUDED.is_group,
            social_connection_id = EXCLUDED.social_connection_id,
            profile_pic_url = EXCLUDED.profile_pic_url,
            created_at_remote = EXCLUDED.created_at_remote,
            updated_at_remote = EXCLUDED.updated_at_remote,
            updated_at = EXCLUDED.updated_at
        `,
        [
          contact.id,
          clientId,
          contact.companyId || null,
          contact.name || null,
          contact.number || null,
          contact.email || null,
          Boolean(contact.isGroup),
          contact.socialConnectionId || null,
          contact.profilePicUrl || null,
          contact.createdAt || null,
          contact.updatedAt || null,
          timestamp,
          timestamp
        ]
      );

      await dbClient.query(`DELETE FROM ${this.schema}.contact_tags WHERE client_id = $1 AND contact_id = $2`, [
        clientId,
        contact.id
      ]);
      await dbClient.query(
        `DELETE FROM ${this.schema}.contact_extra_info WHERE client_id = $1 AND contact_id = $2`,
        [clientId, contact.id]
      );

      for (const tag of contact.tags || []) {
        await dbClient.query(
          `
            INSERT INTO ${this.schema}.tags (
              id, client_id, company_id, name, color, created_at, updated_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7
            )
            ON CONFLICT (client_id, id) DO UPDATE SET
              company_id = EXCLUDED.company_id,
              name = EXCLUDED.name,
              color = EXCLUDED.color,
              updated_at = EXCLUDED.updated_at
          `,
          [tag.id, clientId, tag.companyId || null, tag.name, tag.color || null, timestamp, timestamp]
        );

        await dbClient.query(
          `
            INSERT INTO ${this.schema}.contact_tags (
              client_id, contact_id, tag_id, created_at
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (client_id, contact_id, tag_id) DO NOTHING
          `,
          [clientId, contact.id, tag.id, timestamp]
        );
      }

      for (const item of contact.extraInfo || []) {
        await dbClient.query(
          `
            INSERT INTO ${this.schema}.contact_extra_info (
              extra_info_id, contact_id, client_id, name, value
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (extra_info_id) DO UPDATE SET
              contact_id = EXCLUDED.contact_id,
              client_id = EXCLUDED.client_id,
              name = EXCLUDED.name,
              value = EXCLUDED.value
          `,
          [item.id, contact.id, clientId, item.name, item.value || null]
        );
      }

      await dbClient.query("COMMIT");
    } catch (error) {
      await dbClient.query("ROLLBACK");
      throw error;
    } finally {
      dbClient.release();
    }
  }

  async listTickets(clientId, limit = 50) {
    const result = await this.pool.query(
      `
        SELECT id, external_uuid, external_ticket_id, status, contact_name, contact_number,
               created_at_external, updated_at_external, last_imported_message_created_at
        FROM ${this.schema}.tickets
        WHERE client_id = $1
        ORDER BY updated_at_external DESC NULLS LAST, created_at DESC
        LIMIT $2
      `,
      [clientId, limit]
    );

    return result.rows;
  }

  async listContacts(clientId, limit = 50) {
    const result = await this.pool.query(
      `
        SELECT id, name, number, email, company_id, social_connection_id, updated_at_remote
        FROM ${this.schema}.contacts
        WHERE client_id = $1
        ORDER BY updated_at_remote DESC NULLS LAST, updated_at DESC
        LIMIT $2
      `,
      [clientId, limit]
    );

    return result.rows;
  }

  async getContact(clientId, contactId) {
    const contactResult = await this.pool.query(
      `
        SELECT *
        FROM ${this.schema}.contacts
        WHERE client_id = $1 AND id = $2
      `,
      [clientId, contactId]
    );

    const contact = contactResult.rows[0];
    if (!contact) {
      return null;
    }

    const tagsResult = await this.pool.query(
      `
        SELECT t.id, t.name, t.color, t.company_id
        FROM ${this.schema}.tags t
        JOIN ${this.schema}.contact_tags ct
          ON ct.client_id = t.client_id
         AND ct.tag_id = t.id
        WHERE ct.client_id = $1 AND ct.contact_id = $2
        ORDER BY t.name ASC
      `,
      [clientId, contactId]
    );

    const extraInfoResult = await this.pool.query(
      `
        SELECT extra_info_id, name, value
        FROM ${this.schema}.contact_extra_info
        WHERE client_id = $1 AND contact_id = $2
        ORDER BY extra_info_id ASC
      `,
      [clientId, contactId]
    );

    return {
      ...contact,
      tags: tagsResult.rows,
      extraInfo: extraInfoResult.rows
    };
  }

  async createImportJob({ clientId, page, limit, pages, sweepAll, maxPages, persist }) {
    const result = await this.pool.query(
      `
        INSERT INTO ${this.schema}.import_jobs (
          client_id, status, start_page, current_page, next_ticket_offset, current_ticket_uuid,
          limit_per_page, pages_requested, sweep_all, max_pages, persist, total_pages,
          pages_processed, tickets_seen, tickets_imported, tickets_failed, status_message,
          started_at, updated_at, finished_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9, $10, $11, $12,
          $13, $14, $15, $16, $17,
          $18, $19, $20
        )
        RETURNING *
      `,
      [
        clientId,
        "pending",
        page,
        page,
        0,
        null,
        limit,
        sweepAll ? null : pages,
        sweepAll ? 1 : 0,
        maxPages,
        persist ? 1 : 0,
        null,
        0,
        0,
        0,
        0,
        "Job created",
        nowIso(),
        nowIso(),
        null
      ]
    );

    return result.rows[0];
  }

  async listImportJobs(limit = 20, clientId = null) {
    const params = [limit];
    let whereClause = "";

    if (clientId) {
      params.unshift(clientId);
      whereClause = "WHERE client_id = $1";
    }

    const result = await this.pool.query(
      `
        SELECT *
        FROM ${this.schema}.import_jobs
        ${whereClause}
        ORDER BY job_id DESC
        LIMIT $${params.length}
      `,
      params
    );

    return result.rows;
  }

  async getImportJob(jobId) {
    const result = await this.pool.query(
      `SELECT * FROM ${this.schema}.import_jobs WHERE job_id = $1`,
      [jobId]
    );
    return result.rows[0] || null;
  }

  async getRunningImportJob(clientId = null) {
    const params = [];
    let whereClause = "status = 'running'";

    if (clientId) {
      params.push(clientId);
      whereClause += " AND client_id = $1";
    }

    const result = await this.pool.query(
      `SELECT * FROM ${this.schema}.import_jobs WHERE ${whereClause} ORDER BY job_id DESC LIMIT 1`,
      params
    );
    return result.rows[0] || null;
  }

  async updateImportJob(jobId, fields) {
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined);
    if (entries.length === 0) {
      return this.getImportJob(jobId);
    }

    const params = [];
    const assignments = entries.map(([key, value], index) => {
      params.push(value);
      return `${key} = $${index + 1}`;
    });
    params.push(nowIso(), jobId);
    assignments.push(`updated_at = $${params.length - 1}`);

    const result = await this.pool.query(
      `
        UPDATE ${this.schema}.import_jobs
        SET ${assignments.join(", ")}
        WHERE job_id = $${params.length}
        RETURNING *
      `,
      params
    );

    return result.rows[0] || null;
  }

  async addImportJobError(jobId, { ticketId = null, ticketUuid = null, errorMessage }) {
    await this.pool.query(
      `
        INSERT INTO ${this.schema}.import_job_errors (
          job_id, ticket_id, ticket_uuid, error_message, created_at
        ) VALUES ($1, $2, $3, $4, $5)
      `,
      [jobId, ticketId, ticketUuid, errorMessage, nowIso()]
    );
  }

  async listImportJobErrors(jobId, limit = 100) {
    const result = await this.pool.query(
      `
        SELECT job_error_id, ticket_id, ticket_uuid, error_message, created_at
        FROM ${this.schema}.import_job_errors
        WHERE job_id = $1
        ORDER BY job_error_id DESC
        LIMIT $2
      `,
      [jobId, limit]
    );

    return result.rows;
  }

  async createContactSyncJob({ clientId, page, limit, pages, sweepAll, maxPages }) {
    const result = await this.pool.query(
      `
        INSERT INTO ${this.schema}.contact_sync_jobs (
          client_id, status, start_page, current_page, limit_per_page, pages_requested, sweep_all,
          max_pages, total_pages, pages_processed, contacts_seen, contacts_synced, contacts_failed,
          status_message, started_at, updated_at, finished_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7,
          $8, $9, $10, $11, $12, $13,
          $14, $15, $16, $17
        )
        RETURNING *
      `,
      [
        clientId,
        "pending",
        page,
        page,
        limit,
        sweepAll ? null : pages,
        sweepAll ? 1 : 0,
        maxPages,
        null,
        0,
        0,
        0,
        0,
        "Contact sync job created",
        nowIso(),
        nowIso(),
        null
      ]
    );

    return result.rows[0];
  }

  async listContactSyncJobs(limit = 20, clientId = null) {
    const params = [limit];
    let whereClause = "";

    if (clientId) {
      params.unshift(clientId);
      whereClause = "WHERE client_id = $1";
    }

    const result = await this.pool.query(
      `
        SELECT *
        FROM ${this.schema}.contact_sync_jobs
        ${whereClause}
        ORDER BY job_id DESC
        LIMIT $${params.length}
      `,
      params
    );

    return result.rows;
  }

  async getContactSyncJob(jobId) {
    const result = await this.pool.query(
      `SELECT * FROM ${this.schema}.contact_sync_jobs WHERE job_id = $1`,
      [jobId]
    );
    return result.rows[0] || null;
  }

  async getRunningContactSyncJob(clientId = null) {
    const params = [];
    let whereClause = "status = 'running'";

    if (clientId) {
      params.push(clientId);
      whereClause += " AND client_id = $1";
    }

    const result = await this.pool.query(
      `SELECT * FROM ${this.schema}.contact_sync_jobs WHERE ${whereClause} ORDER BY job_id DESC LIMIT 1`,
      params
    );
    return result.rows[0] || null;
  }

  async updateContactSyncJob(jobId, fields) {
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined);
    if (entries.length === 0) {
      return this.getContactSyncJob(jobId);
    }

    const params = [];
    const assignments = entries.map(([key, value], index) => {
      params.push(value);
      return `${key} = $${index + 1}`;
    });
    params.push(nowIso(), jobId);
    assignments.push(`updated_at = $${params.length - 1}`);

    const result = await this.pool.query(
      `
        UPDATE ${this.schema}.contact_sync_jobs
        SET ${assignments.join(", ")}
        WHERE job_id = $${params.length}
        RETURNING *
      `,
      params
    );

    return result.rows[0] || null;
  }

  async addContactSyncJobError(jobId, { contactId = null, errorMessage }) {
    await this.pool.query(
      `
        INSERT INTO ${this.schema}.contact_sync_job_errors (
          job_id, contact_id, error_message, created_at
        ) VALUES ($1, $2, $3, $4)
      `,
      [jobId, contactId, errorMessage, nowIso()]
    );
  }

  async listContactSyncJobErrors(jobId, limit = 100) {
    const result = await this.pool.query(
      `
        SELECT job_error_id, contact_id, error_message, created_at
        FROM ${this.schema}.contact_sync_job_errors
        WHERE job_id = $1
        ORDER BY job_error_id DESC
        LIMIT $2
      `,
      [jobId, limit]
    );

    return result.rows;
  }

  async close() {
    await this.pool.end();
  }
}
