# Fluxo E SQL Do Importador

## Visao Geral

Este documento descreve o fluxo atual do importador usando Postgres no schema `core`.

Hoje o modo principal e:

1. ler os tenants em `core.sinapse_clients`
2. usar `api_base_url` e `api_key` de cada tenant
3. importar tickets e mensagens
4. transformar `ticketTrakings` em sessoes
5. pre-salvar contatos basicos vindos do ticket
6. sincronizar contatos completos, tags e `extraInfo` em job separado
7. controlar jobs e `resume` por `client_id`

O modo SQLite continua existindo como ambiente local de teste, mas nao e mais o fluxo principal da integracao.

## Fluxo Atual No Postgres Core

### 1. Descoberta do tenant

O servico le os tenants ativos em:

```sql
SELECT id, slug, name, api_base_url, api_key
FROM core.sinapse_clients
WHERE is_active = true;
```

Cada tenant vira uma execucao isolada por `client_id`.

Consequencias praticas:

- cada tenant usa a propria `api_key`
- cada tenant usa a propria `api_base_url`
- os dados ficam segregados por `client_id`
- o bloqueio de job `running` e por tenant, nao global

### 2. Import de tickets

Para um tenant, o job chama:

```http
GET /api/ticket?page={page}&limit={ticketLimit}
```

Regras:

- `ticketLimit` pode ir ate `100`
- cada ticket ja traz `uuid`, `ticketTrakings`, `contact` e metadados gerais
- o job salva checkpoint por pagina e por posicao do ticket dentro da pagina

### 3. Pre-salvamento do contato vindo no ticket

Antes de gravar o snapshot completo:

- o bloco `contact` do ticket e salvo em `core.contacts`
- isso acontece com `ON CONFLICT (client_id, id) DO NOTHING`
- essa etapa nao busca `tags`
- essa etapa nao busca `extraInfo`

Objetivo:

- nao deixar o fluxo principal de tickets mais lento

### 4. Import de mensagens do ticket

Para cada ticket:

```http
GET /api/ticket/{ticketUuid}/messages?page={page}&limit=20
```

Regras:

- o endpoint de mensagens aceita no maximo `20` por pagina
- o cliente do importador ja trava esse limite em `20`
- o servico varre todas as paginas ate terminar o ticket

### 5. Reconstrucao das sessoes

Cada item de `ticketTrakings` gera:

- um registro em `core.imported_trackings`
- uma sessao em `core.sessions`

Mapeamento:

- `finishedAt` presente -> `session_type = CLOSED`
- `startedAt` presente e sem `finishedAt` -> `session_type = OPEN_REAL`
- sem `startedAt` -> `session_type = OPEN_WEAK`

O enum real no banco e:

```sql
core.session_type = ('CLOSED', 'OPEN_REAL', 'OPEN_WEAK', 'UNTRACKED')
```

Hoje o importador usa `CLOSED`, `OPEN_REAL` e `OPEN_WEAK`.

### 6. Distribuicao das mensagens nas sessoes

Regra aplicada:

- a mensagem pertence ao `ticketTracking` mais recente cujo `createdAt` seja menor ou igual ao `message.createdAt`
- e que ainda esteja antes da abertura do proximo `ticketTracking`

Em termos praticos:

- sessao 1 recebe mensagens de `tracking1.createdAt` ate antes de `tracking2.createdAt`
- sessao 2 recebe mensagens de `tracking2.createdAt` ate antes de `tracking3.createdAt`
- e assim por diante

Se nao houver `ticketTrakings`, as mensagens ficam sem `session_id`.

### 7. Persistencia do snapshot do ticket

Ao persistir um ticket:

1. faz upsert em `core.tickets`
2. apaga `messages`, `sessions` e `imported_trackings` anteriores daquele `ticket_id`
3. recria `imported_trackings`
4. recria `sessions`
5. recria `messages`
6. registra a execucao em `core.import_runs`

Chaves deterministicas usadas pelo importador:

- `ticket.id` interno: `ticket:{clientId}:{ticket.uuid}`
- `imported_trackings.id`: `tracking:{clientId}:{ticket.uuid}:{tracking.id}`
- `sessions.id`: `session:{clientId}:{ticket.uuid}:{tracking.id}`
- `messages.id`: `message:{clientId}:{ticket.uuid}:{message.id}`

### 8. Sync separado de contatos, tags e extraInfo

Esse fluxo existe para nao pesar o import de tickets.

O job chama:

```http
GET /api/contact?page={page}&limit={contactLimit}
```

Regras:

- `contactLimit` pode ir ate `100`
- a resposta traz o contato completo
- a resposta traz `tags`
- a resposta traz `extraInfo`

Persistencia:

1. faz upsert em `core.contacts`
2. remove `core.contact_tags` antigos daquele contato
3. remove `core.contact_extra_info` antigos daquele contato
4. faz upsert em `core.tags`
5. recria `core.contact_tags`
6. recria `core.contact_extra_info`

## Jobs E Resume Por Tenant

### Import de tickets

Endpoints:

```http
POST /core/jobs/imports/start?clientId=ferracosul&page=1&ticketLimit=100&pages=all
GET /core/jobs/imports?clientId=ferracosul&limit=20
GET /core/jobs/imports/:jobId
POST /core/jobs/imports/:jobId/resume
```

Checkpoint salvo em `core.import_jobs`:

- `client_id`
- `start_page`
- `current_page`
- `next_ticket_offset`
- `current_ticket_uuid`
- `limit_per_page`
- `pages_requested`
- `sweep_all`
- `max_pages`
- `persist`
- `total_pages`
- `pages_processed`
- `tickets_seen`
- `tickets_imported`
- `tickets_failed`
- `status_message`

Regra de concorrencia:

- so pode existir um import `running` por tenant
- um tenant nao bloqueia o `start` ou `resume` de outro tenant

### Sync de contatos

Endpoints:

```http
POST /core/jobs/contacts/start?clientId=ferracosul&page=1&contactLimit=100&pages=all
GET /core/jobs/contacts?clientId=ferracosul&limit=20
GET /core/jobs/contacts/:jobId
POST /core/jobs/contacts/:jobId/resume
```

Checkpoint salvo em `core.contact_sync_jobs`:

- `client_id`
- `start_page`
- `current_page`
- `limit_per_page`
- `pages_requested`
- `sweep_all`
- `max_pages`
- `total_pages`
- `pages_processed`
- `contacts_seen`
- `contacts_synced`
- `contacts_failed`
- `status_message`

Regra de concorrencia:

- so pode existir um sync de contatos `running` por tenant
- um tenant nao bloqueia o sync de outro tenant

## Limites Da API Externa

O importador usa uma janela deslizante de:

- `60` requisicoes
- em `60` segundos

Funcionamento:

- cada chamada externa consome `1` slot
- ao atingir `60` chamadas na janela atual, a proxima espera
- assim que a janela libera espaco, o fluxo continua sozinho

Limites por endpoint:

- `tickets`: ate `100` por pagina
- `messages`: ate `20` por pagina
- `contacts`: ate `100` por pagina

## SQL E Modelo De Dados Do Core

## Tabela `core.sinapse_clients`

Responsabilidade:

- cadastrar os tenants
- fornecer `api_base_url` e `api_key`

Colunas principais:

```sql
id text primary key,
slug text not null,
name text not null,
domain_uuid text not null,
api_base_url text not null,
api_key text not null,
is_active boolean not null,
created_at timestamp not null,
updated_at timestamp not null
```

## Tabela `core.tickets`

Responsabilidade:

- representar o ticket importado por tenant

Colunas principais:

```sql
id text primary key,
external_uuid text not null,
external_ticket_id integer,
status text,
contact_name text,
contact_number text,
contact_external_id integer,
social_connection_id integer,
company_id integer,
created_at_external timestamp,
updated_at_external timestamp,
last_imported_message_created_at timestamp,
created_at timestamp not null,
updated_at timestamp not null,
client_id text not null,
contact_id bigint,
is_group boolean not null
```

Relacionamentos:

```sql
foreign key (client_id) references core.sinapse_clients(id),
foreign key (client_id, contact_id) references core.contacts(client_id, id)
```

Upsert usado pelo importador:

```sql
ON CONFLICT (client_id, external_uuid) DO UPDATE
```

## Tabela `core.imported_trackings`

Responsabilidade:

- guardar o tracking bruto reconstruido por ticket

Colunas principais:

```sql
id text primary key,
ticket_id text not null,
external_tracking_id integer,
created_at_external timestamp not null,
started_at_external timestamp,
ended_at_external timestamp,
last_rebuild_message_created_at timestamp,
processed_at timestamp,
processing_version text,
processing_notes text,
created_at timestamp not null,
updated_at timestamp not null
```

Relacionamento:

```sql
foreign key (ticket_id) references core.tickets(id)
```

## Tabela `core.sessions`

Responsabilidade:

- representar as sessoes reconstruidas a partir de `ticketTrakings`

Colunas principais:

```sql
id text primary key,
ticket_id text not null,
external_tracking_id integer,
type core.session_type not null,
started_at timestamp not null,
ended_at timestamp,
assigned_user_name text,
assigned_user_email text,
created_at timestamp not null,
updated_at timestamp not null,
origin_imported_tracking_id text,
processing_version text,
source text,
created_at_external timestamp
```

Relacionamentos:

```sql
foreign key (ticket_id) references core.tickets(id),
foreign key (origin_imported_tracking_id) references core.imported_trackings(id)
```

Regra de upsert:

```sql
ON CONFLICT (ticket_id, external_tracking_id) DO UPDATE
```

## Tabela `core.messages`

Responsabilidade:

- guardar todas as mensagens do ticket
- opcionalmente relaciona-las a uma sessao

Colunas principais:

```sql
id text primary key,
ticket_id text not null,
session_id text,
external_message_id text not null,
key text,
body text not null,
from_me boolean not null,
media_url text,
media_type text,
created_at_external timestamp not null,
updated_at_external timestamp not null,
raw_json jsonb,
created_at timestamp not null,
updated_at timestamp not null,
sender_type core.message_sender_type not null
```

Relacionamentos:

```sql
foreign key (ticket_id) references core.tickets(id),
foreign key (session_id) references core.sessions(id)
```

Enum usado:

```sql
core.message_sender_type = ('HUMAN', 'SYSTEM', 'AI')
```

Mapeamento atual:

- `generatedByAi = true` -> `AI`
- `sendBySystem = true` -> `SYSTEM`
- caso contrario -> `HUMAN`

Regra de upsert:

```sql
ON CONFLICT (ticket_id, external_message_id) DO UPDATE
```

## Tabela `core.contacts`

Responsabilidade:

- armazenar o contato por tenant

Colunas principais:

```sql
id bigint not null,
client_id text not null,
company_id bigint,
name text,
number text,
email text,
is_group boolean not null,
social_connection_id bigint,
profile_pic_url text,
created_at_remote timestamp,
updated_at_remote timestamp,
created_at timestamp not null,
updated_at timestamp not null,
primary key (client_id, id)
```

Usos no fluxo:

- pre-save vindo do ticket: `ON CONFLICT (client_id, id) DO NOTHING`
- sync completo: `ON CONFLICT (client_id, id) DO UPDATE`

## Tabela `core.tags`

Responsabilidade:

- armazenar tags de contato por tenant

Colunas principais:

```sql
id bigint not null,
client_id text not null,
company_id bigint,
name text not null,
color text,
created_at timestamp not null,
updated_at timestamp not null,
primary key (client_id, id)
```

## Tabela `core.contact_tags`

Responsabilidade:

- relacionar contatos e tags por tenant

Colunas principais:

```sql
client_id text not null,
contact_id bigint not null,
tag_id bigint not null,
created_at timestamp not null,
primary key (client_id, contact_id, tag_id)
```

Relacionamentos:

```sql
foreign key (client_id, contact_id) references core.contacts(client_id, id),
foreign key (client_id, tag_id) references core.tags(client_id, id)
```

Regra usada:

```sql
ON CONFLICT (client_id, contact_id, tag_id) DO NOTHING
```

## Tabela `core.contact_extra_info`

Responsabilidade:

- guardar os pares nome/valor extras do contato

Colunas principais:

```sql
extra_info_id integer primary key,
contact_id bigint not null,
client_id text not null,
name text not null,
value text
```

Relacionamento:

```sql
foreign key (client_id, contact_id) references core.contacts(client_id, id)
```

Regra usada:

```sql
ON CONFLICT (extra_info_id) DO UPDATE
```

## Tabela `core.import_jobs`

Responsabilidade:

- controlar o import de tickets em background

Colunas principais:

```sql
job_id integer primary key,
status text not null,
start_page integer not null,
current_page integer not null,
next_ticket_offset integer not null,
current_ticket_uuid text,
limit_per_page integer not null,
pages_requested integer,
sweep_all integer not null,
max_pages integer,
persist integer not null,
total_pages integer,
pages_processed integer not null,
tickets_seen integer not null,
tickets_imported integer not null,
tickets_failed integer not null,
status_message text,
started_at text not null,
updated_at text not null,
finished_at text,
client_id text
```

Ajuste aplicado para suporte multi-tenant:

```sql
ALTER TABLE core.import_jobs
ADD COLUMN IF NOT EXISTS client_id text;

CREATE INDEX IF NOT EXISTS idx_import_jobs_client_id
ON core.import_jobs (client_id);
```

## Tabela `core.import_job_errors`

Responsabilidade:

- registrar falhas de import de tickets

Colunas principais:

```sql
job_error_id integer primary key,
job_id integer not null,
ticket_id text,
ticket_uuid text,
error_message text not null,
created_at text not null
```

Relacionamento:

```sql
foreign key (job_id) references core.import_jobs(job_id)
```

## Tabela `core.contact_sync_jobs`

Responsabilidade:

- controlar o sync de contatos em background

Colunas principais:

```sql
job_id integer primary key,
status text not null,
start_page integer not null,
current_page integer not null,
limit_per_page integer not null,
pages_requested integer,
sweep_all integer not null,
max_pages integer,
total_pages integer,
pages_processed integer not null,
contacts_seen integer not null,
contacts_synced integer not null,
contacts_failed integer not null,
status_message text,
started_at text not null,
updated_at text not null,
finished_at text,
client_id text
```

Ajuste aplicado para suporte multi-tenant:

```sql
ALTER TABLE core.contact_sync_jobs
ADD COLUMN IF NOT EXISTS client_id text;

CREATE INDEX IF NOT EXISTS idx_contact_sync_jobs_client_id
ON core.contact_sync_jobs (client_id);
```

## Tabela `core.contact_sync_job_errors`

Responsabilidade:

- registrar falhas do sync de contatos

Colunas principais:

```sql
job_error_id integer primary key,
job_id integer not null,
contact_id bigint,
error_message text not null,
created_at text not null
```

Relacionamento:

```sql
foreign key (job_id) references core.contact_sync_jobs(job_id)
```

## SQL Resumido Das Operacoes De Persistencia

### Ticket

```sql
INSERT INTO core.tickets (...)
VALUES (...)
ON CONFLICT (client_id, external_uuid) DO UPDATE SET ...;
```

### Sessao

```sql
INSERT INTO core.sessions (...)
VALUES (...)
ON CONFLICT (ticket_id, external_tracking_id) DO UPDATE SET ...;
```

### Mensagem

```sql
INSERT INTO core.messages (...)
VALUES (...)
ON CONFLICT (ticket_id, external_message_id) DO UPDATE SET ...;
```

### Contato completo

```sql
INSERT INTO core.contacts (...)
VALUES (...)
ON CONFLICT (client_id, id) DO UPDATE SET ...;
```

### Tag

```sql
INSERT INTO core.tags (...)
VALUES (...)
ON CONFLICT (client_id, id) DO UPDATE SET ...;
```

### Vinculo contato-tag

```sql
INSERT INTO core.contact_tags (...)
VALUES (...)
ON CONFLICT (client_id, contact_id, tag_id) DO NOTHING;
```

### Extra info

```sql
INSERT INTO core.contact_extra_info (...)
VALUES (...)
ON CONFLICT (extra_info_id) DO UPDATE SET ...;
```

## Modo SQLite Local

O SQLite continua existindo para testes locais pelos endpoints antigos:

- `/import/...`
- `/jobs/...`
- `/db/...`

Mas o fluxo oficial da integracao multi-tenant agora e o do Postgres `core`.
