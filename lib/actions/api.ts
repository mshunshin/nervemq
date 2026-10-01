import { z } from "zod";

import type { CreateNamespaceRequest } from "@/lib/schemas/create-namespace";
import type { CreateQueueRequest } from "@/lib/schemas/create-queue";
import type {
  QueueConfig,
  UpdateQueueConfigRequest,
} from "@/lib/schemas/queue-settings";
import { ADMIN_API } from "@/app/globals";
import type { CreateUserRequest } from "@/lib/schemas/create-user";
import type { LoginRequest } from "@/lib/schemas/login-form";
import type { DeleteQueueRequest } from "@/lib/schemas/delete-queue";
import {
  type AdminSession,
  type ApiKey,
  type CreatedApiKey,
  type KeyAccess,
  type MessageListPage,
  type NamespaceMember,
  type NamespaceStatistics,
  type QueueAttributes,
  type QueueStatistics,
  type Role,
  type SettableMessageStatus,
  type StandardQueueAttribute,
  type UserStatistics,
  adminSessionSchema,
  apiKeySchema,
  createdApiKeySchema,
  messageListSchema,
  namespaceMemberSchema,
  namespaceStatisticsSchema,
  queueAttributesSchema,
  queueConfigResponseSchema,
  queueStatisticsSchema,
  sentMessageSchema,
  userStatisticsSchema,
} from "@/lib/types";

/** Shorthand for encoding user-supplied path segments. New names are limited
 *  to letters, digits, `-` and `_` (all URL-safe), but names created over the
 *  API may not be, and encoding keeps that invariant local. */
const seg = encodeURIComponent;

/**
 * Shared fetch wrapper for the admin API: always sends credentials and turns
 * both network failures and non-2xx responses into thrown Errors. Browser
 * fetch resolves successfully on 4xx/5xx, so without the `res.ok` check a
 * failed request would look like a success to callers and TanStack Query.
 */
async function adminFetch(
  path: string,
  init?: RequestInit,
): Promise<Response> {
  const res = await fetch(`${ADMIN_API}${path}`, {
    credentials: "include",
    ...init,
  });
  if (!res.ok) {
    if (res.status === 403) {
      throw new Error("Access Denied");
    }
    // The server explains a rejected request (e.g. "Conflict: … is the last
    // active admin"); show that rather than a bare status code.
    if (res.status === 400 || res.status === 404 || res.status === 409) {
      const message = await res.text().catch(() => "");
      if (message) {
        throw new Error(message);
      }
    }
    throw new Error(`Request failed (${res.status})`);
  }
  return res;
}

export async function logout() {
  await adminFetch("/auth/logout", { method: "POST" });
}

export async function login(data: LoginRequest): Promise<AdminSession> {
  const res = await fetch(`${ADMIN_API}/auth/login`, {
    method: "POST",
    body: JSON.stringify(data),
    credentials: "include",
  });
  if (!res.ok) {
    throw new Error(
      res.status === 401
        ? "Invalid email or password"
        : res.status === 403
          ? "This account is disabled"
          : "Something went wrong",
    );
  }

  return adminSessionSchema.parse(await res.json());
}

export async function createNamespace(data: CreateNamespaceRequest) {
  await adminFetch(`/ns/${seg(data.name)}`, { method: "POST" });
}

export async function deleteNamespace(name: string) {
  await adminFetch(`/ns/${seg(name)}`, { method: "DELETE" });
}

export async function listNamespaces(): Promise<NamespaceStatistics[]> {
  return await adminFetch("/stats/ns")
    .then((res) => res.json())
    .then((json) => namespaceStatisticsSchema.array().parse(json));
}

/** Who has access to a namespace and who owns it (admins and owners). */
export async function listNamespaceMembers(
  namespace: string,
): Promise<NamespaceMember[]> {
  return await adminFetch(`/ns/${seg(namespace)}/members`)
    .then((res) => res.json())
    .then((json) => namespaceMemberSchema.array().parse(json));
}

/** Makes a user an owner of a namespace, or stops them being one (admins).
 *  Becoming an owner grants access; losing ownership keeps it. */
export async function setNamespaceOwner({
  namespace,
  email,
  owner,
}: {
  namespace: string;
  email: string;
  owner: boolean;
}) {
  await adminFetch(`/ns/${seg(namespace)}/owners/${seg(email)}`, {
    method: owner ? "PUT" : "DELETE",
  });
}

export async function listUserAllowedNamespaces({
  email,
}: {
  email?: string;
}): Promise<string[]> {
  if (email === undefined) {
    throw new Error("Email is required");
  }

  return await adminFetch(`/users/${seg(email)}/permissions`)
    .then((res) => res.json())
    .then((json) => z.array(z.string()).parse(json));
}

export async function updateUserAllowedNamespaces({
  email,
  namespaces,
}: {
  email: string;
  namespaces: string[];
}) {
  await adminFetch(`/users/${seg(email)}/permissions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(namespaces),
  });
}

export async function setUserDisabled({
  email,
  disabled,
}: {
  email: string;
  disabled: boolean;
}) {
  await adminFetch(
    `/users/${seg(email)}/${disabled ? "disable" : "enable"}`,
    { method: "POST" },
  );
}

/** Sets another user's password (admins). */
export async function resetUserPassword({
  email,
  password,
}: {
  email: string;
  password: string;
}) {
  await adminFetch(`/users/${seg(email)}/password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password }),
  });
}

/** Changes the logged-in user's own password. */
export async function changeOwnPassword({
  currentPassword,
  newPassword,
}: {
  currentPassword: string;
  newPassword: string;
}) {
  const res = await fetch(`${ADMIN_API}/auth/password`, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      current_password: currentPassword,
      new_password: newPassword,
    }),
  });
  if (!res.ok) {
    throw new Error(
      res.status === 403
        ? "Current password is incorrect"
        : `Request failed (${res.status})`,
    );
  }
}

/** Another user's API keys (admins); never their secrets. */
export async function listUserApiKeys(email: string): Promise<ApiKey[]> {
  return await adminFetch(`/users/${seg(email)}/tokens`)
    .then((res) => res.json())
    .then((json) => apiKeySchema.array().parse(json));
}

/** Revokes another user's API key (admins). */
export async function deleteUserApiKey({
  email,
  name,
}: {
  email: string;
  name: string;
}) {
  await adminFetch(`/users/${seg(email)}/tokens/${seg(name)}`, {
    method: "DELETE",
  });
}

export async function updateUserRole({
  email,
  role,
}: {
  email: string;
  role: Role;
}) {
  await adminFetch(`/users/${seg(email)}/role`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ role }),
  });
}

export async function createQueue(data: CreateQueueRequest) {
  await adminFetch(`/queue/${seg(data.namespace)}/${seg(data.name)}`, {
    method: "POST",
    body: JSON.stringify({
      attributes: Object.fromEntries(data.attributes ?? []),
      tags: Object.fromEntries(data.tags ?? []),
    }),
  });
}

export async function deleteQueue(data: DeleteQueueRequest) {
  await adminFetch(`/queue/${seg(data.namespace)}/${seg(data.name)}`, {
    method: "DELETE",
  });
}

export async function listQueues(): Promise<Map<string, QueueStatistics>> {
  return await adminFetch("/stats/queue")
    .then((res) => res.json())
    .then((json) => z.record(z.string(), queueStatisticsSchema).parse(json))
    .then((record) => new Map(Object.entries(record)));
}

export async function fetchQueue(
  namespace: string,
  queueName: string,
): Promise<QueueStatistics> {
  return await adminFetch(`/queue/${seg(namespace)}/${seg(queueName)}`)
    .then((res) => res.json())
    .then((json) => queueStatisticsSchema.parse(json));
}

/** Sortable columns of the message list (server-side whitelist). */
export type MessageSortKey =
  | "id"
  | "body"
  | "status"
  | "tries"
  | "received_at"
  | "delivered_at";

export async function listMessages({
  queue,
  namespace,
  limit,
  offset,
  sort = "id",
  order = "asc",
}: {
  queue: string;
  namespace: string;
  limit: number;
  offset: number;
  sort?: MessageSortKey;
  order?: "asc" | "desc";
}): Promise<MessageListPage> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    sort,
    order,
  });
  return await adminFetch(
    `/queue/${seg(namespace)}/${seg(queue)}/messages?${params}`,
  )
    .then((res) => res.json())
    .then((json) => messageListSchema.parse(json));
}

export async function purgeQueue({
  namespace,
  queue,
}: {
  namespace: string;
  queue: string;
}) {
  await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/purge`, {
    method: "POST",
  });
}

export async function sendQueueMessage({
  namespace,
  queue,
  body,
  attributes,
}: {
  namespace: string;
  queue: string;
  body: string;
  attributes: Map<string, string>;
}): Promise<{ MessageId: string }> {
  return await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      body,
      // The wire shape mirrors SQS message attributes; the UI only sends
      // String-typed values.
      attributes: Object.fromEntries(
        Array.from(attributes.entries()).map(([key, value]) => [
          key,
          { DataType: "String", StringValue: value },
        ]),
      ),
    }),
  })
    .then((res) => res.json())
    .then((json) => sentMessageSchema.parse(json));
}

export async function deleteQueueMessage({
  namespace,
  queue,
  id,
}: {
  namespace: string;
  queue: string;
  id: number;
}) {
  await adminFetch(
    `/queue/${seg(namespace)}/${seg(queue)}/messages/${seg(String(id))}`,
    { method: "DELETE" },
  );
}

/** Deletes every failed (retry-exhausted) message in the queue and returns
 * how many were removed. A queue with no failed messages is a no-op. */
export async function clearFailedMessages({
  namespace,
  queue,
}: {
  namespace: string;
  queue: string;
}): Promise<{ deleted: number }> {
  const res = await adminFetch(
    `/queue/${seg(namespace)}/${seg(queue)}/messages/failed`,
    { method: "DELETE" },
  );
  return clearedMessagesSchema.parse(await res.json());
}

const clearedMessagesSchema = z.object({ deleted: z.number() });

export async function updateMessageStatus({
  namespace,
  queue,
  id,
  status,
}: {
  namespace: string;
  queue: string;
  id: number;
  status: SettableMessageStatus;
}) {
  await adminFetch(
    `/queue/${seg(namespace)}/${seg(queue)}/messages/${seg(String(id))}/status`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    },
  );
}

export async function getQueueAttributes(
  namespace: string,
  queue: string,
): Promise<QueueAttributes> {
  return await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/attributes`)
    .then((res) => res.json())
    .then((json) => queueAttributesSchema.parse(json));
}

export async function setQueueAttributes({
  namespace,
  queue,
  attributes,
}: {
  namespace: string;
  queue: string;
  attributes: Partial<Record<StandardQueueAttribute, string>>;
}) {
  await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/attributes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(attributes),
  });
}

export async function listAPIKeys(): Promise<ApiKey[]> {
  return await adminFetch("/tokens")
    .then((res) => res.json())
    .then((json) => apiKeySchema.array().parse(json));
}

export type CreateTokenRequest = {
  name: string;
  namespace: string;
  /** At most the caller's own level in the namespace; omitted, it is that. */
  access?: KeyAccess;
};

export async function createAPIKey(
  req: CreateTokenRequest,
): Promise<CreatedApiKey> {
  return await adminFetch("/tokens", {
    method: "POST",
    body: JSON.stringify(req),
  })
    .then((res) => res.json())
    .then((json) => createdApiKeySchema.parse(json));
}

export type DeleteTokenRequest = {
  name: string;
};

export async function deleteAPIKey(req: DeleteTokenRequest) {
  await adminFetch("/tokens", {
    method: "DELETE",
    body: JSON.stringify(req),
  });
}

export async function createUser(data: CreateUserRequest): Promise<void> {
  await adminFetch("/users", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
}

export type DeleteUserRequest = {
  email: string;
};

export async function deleteUser(data: DeleteUserRequest) {
  await adminFetch("/users", {
    method: "DELETE",
    body: JSON.stringify(data),
  });
}

export async function listUsers(): Promise<UserStatistics[]> {
  return await adminFetch("/users")
    .then((res) => res.json())
    .then((json) => userStatisticsSchema.array().parse(json));
}

export async function updateQueueSettings(data: UpdateQueueConfigRequest) {
  await adminFetch(`/queue/${seg(data.namespace)}/${seg(data.queue)}/config`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      max_retries: data.maxRetries,
      dead_letter_queue: data.deadLetterQueue,
    }),
  });
}

export async function getQueueSettings(
  namespace?: string,
  queue?: string,
): Promise<QueueConfig> {
  if (namespace === undefined || queue === undefined) {
    throw new Error("Invalid queue ID");
  }
  return await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/config`)
    .then((res) => res.json())
    .then((json) => queueConfigResponseSchema.parse(json))
    .then((data) => ({
      maxRetries: data.max_retries,
      deadLetterQueue: data.dead_letter_queue ?? undefined,
    }));
}
