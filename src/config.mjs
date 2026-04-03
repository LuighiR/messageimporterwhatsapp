import { loadRootEnvFile } from "./env.mjs";

const DEFAULT_BASE_URL = "https://atende-api.corz.com.br/api";
const DEFAULT_PORT = 3000;
const DEFAULT_DATABASE_PATH = "data/importer.sqlite";
const DEFAULT_POSTGRES_SCHEMA = "core";
const DEFAULT_RATE_LIMIT_REQUESTS = 60;
const DEFAULT_RATE_LIMIT_WINDOW_MS = 60_000;

loadRootEnvFile();

export function getConfig() {
  const apiKey = process.env.CORZ_API_KEY || process.env.corz_api_key;

  return {
    apiKey,
    baseUrl: process.env.CORZ_BASE_URL || DEFAULT_BASE_URL,
    port: Number.parseInt(process.env.PORT || `${DEFAULT_PORT}`, 10),
    databasePath: process.env.DATABASE_PATH || DEFAULT_DATABASE_PATH,
    postgresUrl: process.env.POSTGRES_URL || process.env.DATABASE_URL || null,
    postgresSchema: process.env.POSTGRES_SCHEMA || DEFAULT_POSTGRES_SCHEMA,
    rateLimitRequests: Number.parseInt(
      process.env.CORZ_RATE_LIMIT_REQUESTS || `${DEFAULT_RATE_LIMIT_REQUESTS}`,
      10
    ),
    rateLimitWindowMs: Number.parseInt(
      process.env.CORZ_RATE_LIMIT_WINDOW_MS || `${DEFAULT_RATE_LIMIT_WINDOW_MS}`,
      10
    )
  };
}
