# dkwMessageImporterTest

Microservico local de teste para validar o fluxo:

1. importar tickets pela API Corz
2. transformar `ticketTrakings` em sessoes
3. importar todas as mensagens de um ticket
4. distribuir as mensagens nas sessoes por janela de tempo

## Configuracao

Defina a chave no arquivo `.env` na raiz do projeto:

```dotenv
corz_api_key=SUA_API_KEY
```

Depois inicie:

```powershell
npm start
```

Opcionalmente, voce tambem pode definir:

```powershell
$env:PORT="4000"
$env:CORZ_BASE_URL="https://atende-api.corz.com.br/api"
$env:DATABASE_PATH="data/importer.sqlite"
$env:POSTGRES_URL="postgres://usuario:senha@host:5432/database?sslmode=disable"
$env:POSTGRES_SCHEMA="core"
$env:CORZ_RATE_LIMIT_REQUESTS="60"
$env:CORZ_RATE_LIMIT_WINDOW_MS="60000"
```

## Integracao Postgres Core

Quando `POSTGRES_URL` estiver configurado, o servico habilita a integracao com o schema `core`.

Nesse modo:

- os tenants ativos sao lidos de `core.sinapse_clients`
- cada tenant fornece `api_base_url` e `api_key`
- os imports gravam em `core.tickets`, `core.sessions`, `core.messages`, `core.contacts`, `core.tags` e tabelas relacionadas
- os jobs passam a ser disparados por `clientId`
- o bloqueio de `running` agora e por tenant, entao um tenant nao impede o `resume` ou `start` de outro

### Listar tenants ativos

```http
GET /core/clients
```

### Import de tickets para um tenant

```http
POST /core/jobs/imports/start?clientId=ferracosul&page=1&ticketLimit=100&pages=all
```

### Consultar job de tickets

```http
GET /core/jobs/imports?clientId=ferracosul&limit=20
GET /core/jobs/imports/:jobId
POST /core/jobs/imports/:jobId/resume
```

### Sync de contatos para um tenant

```http
POST /core/jobs/contacts/start?clientId=ferracosul&page=1&contactLimit=100&pages=all
```

### Consultar job de contatos

```http
GET /core/jobs/contacts?clientId=ferracosul&limit=20
GET /core/jobs/contacts/:jobId
POST /core/jobs/contacts/:jobId/resume
```

### Consultar dados persistidos no Postgres

```http
GET /core/db/tickets?clientId=ferracosul&limit=50
GET /core/db/contacts?clientId=ferracosul&limit=50
GET /core/db/contacts/:contactId?clientId=ferracosul
```

## Endpoints locais

### Health

```http
GET /health
```

### Importar tickets e montar sessoes

```http
GET /import/tickets?page=1&ticketLimit=100
```

Resposta:

- `ticket`: dados normalizados do ticket
- `sessions`: sessoes derivadas de `ticketTrakings`
- `ticketLimit` vale para a API de tickets, que aceita `100`
- o contato basico vindo do ticket e pre-salvo no SQLite se ainda nao existir

### Importar mensagens de um ticket

```http
GET /import/tickets/:ticketUuid/messages?page=1&limit=20
```

Resposta:

- payload bruto da API de mensagens
- o servico limita automaticamente `limit` em `20`, que e o maximo aceito pela API

### Snapshot completo do import

```http
GET /import/tickets/:ticketUuid/snapshot?ticketPage=1&ticketLimit=100
```

Resposta:

- `ticket`: ticket normalizado
- `sessions`: sessoes derivadas de `ticketTrakings`
- `messageCount`: total de mensagens importadas
- `sessionAssignmentRule`: regra aplicada para vincular mensagens a sessoes

### Importar e persistir no SQLite

```http
POST /import/tickets/:ticketUuid/persist?ticketPage=1&ticketLimit=100
```

Resposta:

- `persisted`: resumo da gravacao no banco
- `snapshot`: payload normalizado usado para persistencia

### Fluxo automatico em lote

```http
POST /import/batch?page=1&ticketLimit=100&pages=1
```

Para varrer tudo:

```http
POST /import/batch?page=1&ticketLimit=100&pages=all
```

Para testar antes com um freio:

```http
POST /import/batch?page=1&ticketLimit=100&pages=all&maxPages=10
```

Esse endpoint executa o fluxo completo sozinho:

- busca os tickets na pagina informada
- para cada ticket, usa o `uuid` para buscar todas as mensagens
- transforma `ticketTrakings` em sessoes
- distribui as mensagens entre as sessoes
- persiste tudo no SQLite

Parametros:

- `page`: pagina inicial dos tickets
- `ticketLimit`: quantidade de tickets por pagina
- `pages`: quantas paginas consecutivas importar, ou `all` para varrer ate a ultima pagina
- `maxPages`: freio opcional quando usar `pages=all`
- `persist=false`: executa o fluxo sem gravar no SQLite

### Job com checkpoint e resume

Para o varrimento total com retomada segura, use o job assíncrono:

```http
POST /jobs/imports/start?page=1&ticketLimit=100&pages=all
```

Consultar status:

```http
GET /jobs/imports/:jobId
```

Listar jobs:

```http
GET /jobs/imports?limit=20
```

Retomar um job pausado ou falho:

```http
POST /jobs/imports/:jobId/resume
```

Retomar limpando um freio anterior de paginas:

```http
POST /jobs/imports/:jobId/resume?maxPages=0
```

Como funciona:

- o job roda em background e a resposta do `start` volta na hora
- o checkpoint fica salvo no SQLite por pagina e posicao do ticket
- se o processo cair, voce sobe o servico de novo e chama `resume`
- o job guarda contadores de paginas, tickets importados, falhas e o ticket atual
- apenas um job pode ficar `running` por vez
- se voce iniciou com `maxPages`, pode limpar esse limite no `resume?maxPages=0`

### Consultar tickets persistidos

```http
GET /db/tickets?limit=50
```

### Consultar snapshot persistido de um ticket

```http
GET /db/tickets/:ticketUuid
```

## Regra de distribuicao das mensagens

As mensagens sao ordenadas por `createdAt` e atribuidas para a sessao mais recente cujo `ticketTracking.createdAt` seja menor ou igual ao `message.createdAt`, desde que ainda nao tenha sido aberta a proxima sessao.

Em termos praticos:

- sessao 1 recebe mensagens de `tracking1.createdAt` ate antes de `tracking2.createdAt`
- sessao 2 recebe mensagens de `tracking2.createdAt` ate antes de `tracking3.createdAt`
- e assim por diante

Se o ticket nao tiver `ticketTrakings`, as mensagens ficam em `unassignedMessages`.

## Persistencia SQLite

O servico cria automaticamente o arquivo SQLite em `data/importer.sqlite` por padrao.

Tabelas criadas:

- `tickets`
- `sessions`
- `messages`
- `import_runs`
- `contacts`
- `tags`
- `contact_tags`
- `contact_extra_info`
- `import_jobs`
- `import_job_errors`
- `contact_sync_jobs`
- `contact_sync_job_errors`

## Contatos e tags

No fluxo de tickets:

- o ticket ja traz um bloco `contact`
- esse contato basico e pre-salvo no SQLite se ainda nao existir
- isso nao busca tags nem `extraInfo`, para nao deixar o import de tickets mais lento

Para atualizar todos os contatos com tags e `extraInfo`, use o job separado:

```http
POST /jobs/contacts/start?page=1&contactLimit=100&pages=all
```

Consultar status:

```http
GET /jobs/contacts/:jobId
```

Retomar:

```http
POST /jobs/contacts/:jobId/resume
```

Retomar limpando um freio anterior:

```http
POST /jobs/contacts/:jobId/resume?maxPages=0
```

Consultar no banco:

```http
GET /db/contacts?limit=50
GET /db/contacts/:contactId
```

## Limite da API externa

O cliente da API Corz agora respeita um limite de `60` requisicoes por `60` segundos antes de chamar o endpoint externo.

Na pratica:

- cada chamada para `GET /import/tickets` consome `1` requisicao externa
- cada chamada para `GET /import/tickets/:ticketUuid/messages` consome `1` requisicao externa
- cada chamada para `GET /import/tickets/:ticketUuid/snapshot` consome `1` requisicao de ticket mais `N` requisicoes de mensagens
- cada chamada para `POST /import/tickets/:ticketUuid/persist` consome `1` requisicao de ticket mais `N` requisicoes de mensagens
- a API de `tickets` aceita `ticketLimit=100`
- a API de `messages` aceita no maximo `20` por pagina e o cliente ja trava nisso automaticamente

Como o endpoint de mensagens aceita no maximo `20` itens por pagina, o total de requisicoes para mensagens fica:

- `1` pagina de mensagens = `1` requisicao
- `100` mensagens = `5` requisicoes
- `500` mensagens = `25` requisicoes

Se a janela de 60 requisicoes estiver cheia, o servico espera automaticamente antes de continuar.
