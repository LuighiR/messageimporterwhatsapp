function toIsoOrNull(value) {
  return value ? new Date(value).toISOString() : null;
}

function toTimestamp(value) {
  if (!value) {
    return null;
  }

  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function buildSessions(ticket) {
  const sortedTrackings = [...(ticket.ticketTrakings || [])].sort((left, right) => {
    return toTimestamp(left.createdAt) - toTimestamp(right.createdAt);
  });

  return sortedTrackings.map((tracking, index) => {
    const nextTracking = sortedTrackings[index + 1];

    return {
      sessionId: tracking.id,
      ticketTrackingId: tracking.id,
      ticketId: ticket.id,
      ticketUuid: ticket.uuid,
      sequence: index + 1,
      openedAt: toIsoOrNull(tracking.createdAt),
      startedAt: toIsoOrNull(tracking.startedAt),
      finishedAt: toIsoOrNull(tracking.finishedAt),
      nextSessionOpenedAt: toIsoOrNull(nextTracking?.createdAt || null),
      assignedUser: tracking.user
        ? {
            id: tracking.user.id,
            name: tracking.user.name,
            email: tracking.user.email
          }
        : null,
      messageCount: 0,
      messages: []
    };
  });
}

function normalizeMessage(message) {
  return {
    id: message.id,
    ticketId: message.ticketId,
    body: message.body,
    mediaType: message.mediaType,
    mediaUrl: message.mediaUrl,
    fromMe: Boolean(message.fromMe),
    sendBySystem: Boolean(message.sendBySystem),
    createdAt: toIsoOrNull(message.createdAt),
    updatedAt: toIsoOrNull(message.updatedAt),
    remoteJid: message.remoteJid,
    ack: message.ack,
    read: Boolean(message.read),
    quotedMsgId: message.quotedMsgId,
    isDeleted: Boolean(message.isDeleted),
    key: message.key
  };
}

function assignMessagesToSessions(sessions, normalizedMessages) {
  if (sessions.length === 0) {
    return [];
  }

  const sessionsWithMessages = sessions.map((session) => ({ ...session, messages: [] }));

  for (const message of normalizedMessages) {
    const messageTimestamp = toTimestamp(message.createdAt);
    let targetSession = sessionsWithMessages[sessionsWithMessages.length - 1];

    for (let index = 0; index < sessionsWithMessages.length; index += 1) {
      const currentSession = sessionsWithMessages[index];
      const nextSession = sessionsWithMessages[index + 1];
      const currentTimestamp = toTimestamp(currentSession.openedAt);
      const nextTimestamp = toTimestamp(nextSession?.openedAt || null);

      if (currentTimestamp !== null && messageTimestamp !== null && messageTimestamp >= currentTimestamp) {
        if (nextTimestamp === null || messageTimestamp < nextTimestamp) {
          targetSession = currentSession;
          break;
        }
      }
    }

    targetSession.messages.push(message);
  }

  return sessionsWithMessages.map((session) => ({
    ...session,
    messageCount: session.messages.length
  }));
}

export function normalizeTicket(ticket) {
  return {
    id: ticket.id,
    uuid: ticket.uuid,
    status: ticket.status,
    createdAt: toIsoOrNull(ticket.createdAt),
    updatedAt: toIsoOrNull(ticket.updatedAt),
    lastMessage: ticket.lastMessage,
    lastMessageHour: toIsoOrNull(ticket.lastMessageHour),
    unreadMessages: ticket.unreadMessages,
    isGroup: Boolean(ticket.isGroup),
    fromMe: Boolean(ticket.fromMe),
    companyId: ticket.companyId,
    tenantId: ticket.tenantId,
    queueId: ticket.queueId,
    queueOptionId: ticket.queueOptionId,
    contact: ticket.contact
      ? {
          id: ticket.contact.id,
          name: ticket.contact.name,
          number: ticket.contact.number,
          email: ticket.contact.email
        }
      : null,
    socialConnection: ticket.socialConnection
      ? {
          id: ticket.socialConnection.id,
          name: ticket.socialConnection.name,
          platform: ticket.socialConnection.platform
        }
      : null
  };
}

export function buildTicketImportSnapshot(ticket, messages) {
  const sessions = buildSessions(ticket);
  const normalizedMessages = [...messages]
    .map(normalizeMessage)
    .sort((left, right) => toTimestamp(left.createdAt) - toTimestamp(right.createdAt));
  const sessionsWithMessages = assignMessagesToSessions(sessions, normalizedMessages);

  return {
    ticket: normalizeTicket(ticket),
    sessionCount: sessionsWithMessages.length,
    messageCount: normalizedMessages.length,
    sessionAssignmentRule: "A message belongs to the latest ticketTracking whose createdAt is <= message.createdAt and before the next ticketTracking createdAt.",
    sessions: sessionsWithMessages,
    unassignedMessages: sessionsWithMessages.length === 0 ? normalizedMessages : []
  };
}
