import type { NamespaceStatistics } from "@/components/namespaces/table";
import type { QueueStatistics } from "@/components/queues/table";
import type { CreateNamespaceRequest } from "@/lib/schemas/create-namespace";
import type { CreateQueueRequest } from "@/lib/schemas/create-queue";
import type {
  QueueConfig,
  UpdateQueueConfigRequest,
} from "@/lib/schemas/queue-settings";
import type { APIKey } from "@/components/create-api-key";
import type { UserStatistics } from "@/components/create-user";
import { ADMIN_API } from "@/app/globals";
import type { CreateUserRequest } from "@/lib/schemas/create-user";
import type { ApiKey } from "@/components/api-keys/table";
import type { AdminSession, Role } from "@/lib/state/global";
import type { MessageObject } from "@/app/(dashboard)/queues/list";
import type { LoginRequest } from "@/lib/schemas/login-form";
import type { DeleteQueueRequest } from "@/lib/schemas/delete-queue";

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

  return await res.json();
}

export async function createNamespace(data: CreateNamespaceRequest) {
  await adminFetch(`/ns/${data.name}`, { method: "POST" });
}

export async function deleteNamespace(name: string) {
  await adminFetch(`/ns/${name}`, { method: "DELETE" });
}

export async function listNamespaces(): Promise<NamespaceStatistics[]> {
  return await adminFetch("/stats/ns").then((res) => res.json());
}

export async function listUserAllowedNamespaces({
  email,
}: {
  email?: string;
}): Promise<string[]> {
  if (email === undefined) {
    throw new Error("Email is required");
  }

  return await adminFetch(
    `/users/${encodeURIComponent(email)}/permissions`,
  ).then((res) => res.json());
}

export async function updateUserAllowedNamespaces({
  email,
  namespaces,
}: {
  email: string;
  namespaces: string[];
}) {
  await adminFetch(`/users/${encodeURIComponent(email)}/permissions`, {
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
  await adminFetch(`/users/${encodeURIComponent(email)}/role`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ role }),
  });
}

export async function createQueue(data: CreateQueueRequest) {
  await adminFetch(`/queue/${data.namespace}/${data.name}`, {
    method: "POST",
    body: JSON.stringify({
      attributes: Object.fromEntries(data.attributes ?? []),
      tags: Object.fromEntries(data.tags ?? []),
    }),
  });
}

export async function deleteQueue(data: DeleteQueueRequest) {
  await adminFetch(`/queue/${data.namespace}/${data.name}`, {
    method: "DELETE",
  });
}

export async function listQueues(): Promise<Map<string, QueueStatistics>> {
  return await adminFetch("/stats/queue")
    .then((res) => res.json())
    .then(
      (json: Record<string, QueueStatistics>) => new Map(Object.entries(json)),
    );
}

export async function fetchQueue(
  namespace: string,
  queueName: string,
): Promise<QueueStatistics> {
  return await adminFetch(`/queue/${namespace}/${queueName}`).then((res) =>
    res.json(),
  );
}

export async function listMessages({
  queue,
  namespace,
}: {
  queue: string;
  namespace: string;
}): Promise<MessageObject[]> {
  return await adminFetch(`/queue/${namespace}/${queue}/messages`).then(
    (res) => res.json(),
  );
}

export async function listAPIKeys(): Promise<ApiKey[]> {
  return await adminFetch("/tokens").then((res) => res.json());
}

export type CreateTokenRequest = {
  name: string;
};

export async function createAPIKey(req: CreateTokenRequest): Promise<APIKey> {
  return await adminFetch("/tokens", {
    method: "POST",
    body: JSON.stringify(req),
  }).then((res) => res.json());
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
  return await adminFetch("/users").then((res) => res.json());
}

export async function updateQueueSettings(data: UpdateQueueConfigRequest) {
  await adminFetch(`/queue/${data.namespace}/${data.queue}/config`, {
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
  return await adminFetch(`/queue/${namespace}/${queue}/config`)
    .then((res) => res.json())
    .then((data) => ({
      maxRetries: data.max_retries,
      deadLetterQueue: data.dead_letter_queue,
    }));
}
