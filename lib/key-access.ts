import { KEY_ACCESS_RANK, type KeyAccess } from "@/lib/types";

/** How each API key access level is shown, from most to least access. */
export const KEY_ACCESS_LEVELS: {
  value: KeyAccess;
  label: string;
  description: string;
}[] = [
  {
    value: "admin",
    label: "Admin",
    description: "Everything you can do, including the admin API",
  },
  {
    value: "owner",
    label: "Owner",
    description: "Send and receive, and manage the namespace's queues",
  },
  {
    value: "member",
    label: "Member",
    description: "Send and receive messages only",
  },
];

/** The label for a key access level. */
export function keyAccessLabel(access: KeyAccess): string {
  return KEY_ACCESS_LEVELS.find((level) => level.value === access)?.label ?? access;
}

/**
 * The most access a key may have in a namespace: the caller's own level
 * there. Admins may grant any level, a namespace's owners `owner`, and other
 * members `member`.
 */
export function maxKeyAccess(isAdmin: boolean, canManage: boolean): KeyAccess {
  if (isAdmin) return "admin";
  return canManage ? "owner" : "member";
}

/** `access` lowered to at most `max`. */
export function capKeyAccess(access: KeyAccess, max: KeyAccess): KeyAccess {
  return KEY_ACCESS_RANK[access] > KEY_ACCESS_RANK[max] ? max : access;
}
