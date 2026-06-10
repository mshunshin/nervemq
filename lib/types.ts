import { z } from "zod";

/**
 * Wire types for the admin API, with zod schemas used by lib/actions/api.ts
 * to validate responses at the boundary. Field names mirror the Rust structs
 * (snake_case) — see src/queue.rs, src/namespace.rs, src/api/tokens.rs.
 */

export enum Role {
  Admin = "admin",
  User = "user",
}

export const roleSchema = z.enum(Role);

export const adminSessionSchema = z.object({
  email: z.string(),
  role: roleSchema,
});

export type AdminSession = z.infer<typeof adminSessionSchema>;

export const namespaceStatisticsSchema = z.object({
  id: z.number(),
  name: z.string(),
  created_by: z.string(),
  queue_count: z.number(),
});

export type NamespaceStatistics = z.infer<typeof namespaceStatisticsSchema>;

export const queueStatisticsSchema = z.object({
  id: z.number(),
  ns: z.string(),
  name: z.string(),
  created_by: z.string(),
  message_count: z.number(),
  avg_size_bytes: z.number(),
  pending: z.number(),
  delivered: z.number(),
  failed: z.number(),
});

export type QueueStatistics = z.infer<typeof queueStatisticsSchema>;

export const userStatisticsSchema = z.object({
  email: z.string(),
  role: roleSchema,
});

export type UserStatistics = z.infer<typeof userStatisticsSchema>;

/** A listed API key (GET /tokens). */
export const apiKeySchema = z.object({
  name: z.string(),
  namespace: z.string(),
});

export type ApiKey = z.infer<typeof apiKeySchema>;

/** The one-time response to creating an API key (POST /tokens). */
export const createdApiKeySchema = z.object({
  name: z.string(),
  namespace: z.string(),
  access_key: z.string(),
  secret_key: z.string(),
});

export type CreatedApiKey = z.infer<typeof createdApiKeySchema>;

export const messageObjectSchema = z.object({
  id: z.number(),
  queue: z.string(),
  body: z.string(),
  tries: z.number(),
  delivered_at: z.number(),
  sent_by: z.string().nullable(),
  status: z.enum(["pending", "delivered", "failed"]),
  message_attributes: z.record(z.string(), z.union([z.string(), z.number()])),
});

export type MessageObject = z.infer<typeof messageObjectSchema>;

export const queueConfigResponseSchema = z.object({
  queue: z.number(),
  max_retries: z.number(),
  dead_letter_queue: z.string().nullable(),
});
