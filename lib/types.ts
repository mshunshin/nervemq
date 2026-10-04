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
  // The creating admin's email; null only for namespaces whose creator was
  // deleted before migration 0011 started recording it.
  created_by: z.string().nullable(),
  queue_count: z.number(),
  // Emails of the users who own the namespace (possibly none).
  owners: z.array(z.string()),
  // Whether the caller may delete the namespace and manage its queues: they
  // are an admin or an owner. Otherwise they may only send and receive.
  can_manage: z.boolean(),
});

export type NamespaceStatistics = z.infer<typeof namespaceStatisticsSchema>;

export const queueStatisticsSchema = z.object({
  id: z.number(),
  ns: z.string(),
  name: z.string(),
  // Null once the creating user has been deleted.
  created_by: z.string().nullable(),
  // When the queue was paused (unix seconds); null while it is running. A
  // paused queue accepts messages and acknowledgements but hands none out.
  paused_at: z.number().nullable(),
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
  // Disabled users can neither log in nor use their API keys.
  disabled: z.boolean(),
});

export type UserStatistics = z.infer<typeof userStatisticsSchema>;

/** A user with access to a namespace (GET /ns/{name}/members). */
export const namespaceMemberSchema = z.object({
  email: z.string(),
  owner: z.boolean(),
});

export type NamespaceMember = z.infer<typeof namespaceMemberSchema>;

/**
 * The most an API key may do, from least to most. It caps its owner's own
 * level and never raises it.
 */
export const keyAccessSchema = z.enum(["member", "owner", "admin"]);

export type KeyAccess = z.infer<typeof keyAccessSchema>;

/** Ranks key access levels for comparison. */
export const KEY_ACCESS_RANK: Record<KeyAccess, number> = {
  member: 0,
  owner: 1,
  admin: 2,
};

/** A listed API key (GET /tokens, GET /users/{email}/tokens). */
export const apiKeySchema = z.object({
  name: z.string(),
  namespace: z.string(),
  access: keyAccessSchema,
});

export type ApiKey = z.infer<typeof apiKeySchema>;

/** The one-time response to creating an API key (POST /tokens). */
export const createdApiKeySchema = z.object({
  name: z.string(),
  namespace: z.string(),
  access: keyAccessSchema,
  access_key: z.string(),
  secret_key: z.string(),
});

export type CreatedApiKey = z.infer<typeof createdApiKeySchema>;

export const messageObjectSchema = z.object({
  // The MessageId: a UUID, as SQS returns it.
  id: z.string(),
  queue: z.string(),
  body: z.string(),
  tries: z.number(),
  // When the queue received the message; null only for rows that predate
  // migration 0006 (Option<u64> in src/message.rs).
  received_at: z.number().nullable(),
  // Null until the message is first delivered (Option<u64> in src/message.rs).
  delivered_at: z.number().nullable(),
  // User id of the sender, if any (Option<u64> in src/message.rs).
  sent_by: z.number().nullable(),
  status: z.enum(["pending", "delivered", "failed"]),
  message_attributes: z.record(z.string(), z.union([z.string(), z.number()])),
});

export type MessageObject = z.infer<typeof messageObjectSchema>;

/**
 * One page of a queue's messages (GET /queue/{ns}/{name}/messages with
 * ?limit=&offset=); `total` is the whole queue's message count.
 */
export const messageListSchema = z.object({
  messages: z.array(messageObjectSchema),
  total: z.number(),
});

export type MessageListPage = z.infer<typeof messageListSchema>;

export const queueConfigResponseSchema = z.object({
  queue: z.number(),
  max_retries: z.number(),
  dead_letter_queue: z.string().nullable(),
});

/** A message's lifecycle states; only these two can be set from the UI. */
export type SettableMessageStatus = "pending" | "failed";

/**
 * Queue attributes in the SQS wire shape (GET/POST
 * /queue/{ns}/{name}/attributes). Values travel as strings, AWS-style;
 * unset attributes are omitted. Custom attributes may also appear.
 */
export const queueAttributesSchema = z
  .object({
    VisibilityTimeout: z.string().optional(),
    DelaySeconds: z.string().optional(),
    MaximumMessageSize: z.string().optional(),
    MessageRetentionPeriod: z.string().optional(),
    ReceiveMessageWaitTimeSeconds: z.string().optional(),
    RedrivePolicy: z.string().optional(),
  })
  .catchall(z.unknown());

export type QueueAttributes = z.infer<typeof queueAttributesSchema>;

/** The standard, editable attributes and their display labels. */
export const STANDARD_QUEUE_ATTRIBUTES = [
  ["VisibilityTimeout", "Visibility Timeout (s)"],
  ["DelaySeconds", "Delivery Delay (s)"],
  ["MaximumMessageSize", "Maximum Message Size (bytes)"],
  ["MessageRetentionPeriod", "Message Retention Period (s)"],
  ["ReceiveMessageWaitTimeSeconds", "Receive Wait Time (s)"],
] as const;

export type StandardQueueAttribute =
  (typeof STANDARD_QUEUE_ATTRIBUTES)[number][0];

/** Response to enqueuing a message (POST /queue/{ns}/{name}/messages). */
export const sentMessageSchema = z.object({
  MessageId: z.string(),
});
