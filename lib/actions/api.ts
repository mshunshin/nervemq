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
  type MessageObject,
  type NamespaceStatistics,
  type QueueStatistics,
  type Role,
  type UserStatistics,
  adminSessionSchema,
  apiKeySchema,
  createdApiKeySchema,
  messageObjectSchema,
  namespaceStatisticsSchema,
  queueConfigResponseSchema,
  queueStatisticsSchema,
  userStatisticsSchema,
} from "@/lib/types";

/** Shorthand for encoding user-supplied path segments. Names are validated
 *  as alphanumeric today, but encoding keeps that invariant local. */
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
      res.status === 401 ? "Invalid email or password" : "Something went wrong",
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

export async function listMessages({
  queue,
  namespace,
}: {
  queue: string;
  namespace: string;
}): Promise<MessageObject[]> {
  return await adminFetch(`/queue/${seg(namespace)}/${seg(queue)}/messages`)
    .then((res) => res.json())
    .then((json) => messageObjectSchema.array().parse(json));
}

export async function listAPIKeys(): Promise<ApiKey[]> {
  return await adminFetch("/tokens")
    .then((res) => res.json())
    .then((json) => apiKeySchema.array().parse(json));
}

export type CreateTokenRequest = {
  name: string;
  namespace: string;
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
