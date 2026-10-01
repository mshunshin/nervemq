import { z } from "zod";

/**
 * Letters, digits, hyphens and underscores — the alphabet AWS SQS allows in
 * queue names. All of them are safe in queue URLs
 * (`/api/sqs/<namespace>/<queue>`) and admin API paths.
 */
const NAME_PATTERN = /^[A-Za-z0-9_-]+$/;
const NAME_MESSAGE = "Use only letters, digits, hyphens (-) and underscores (_)";

/** A new queue's name: up to 80 characters, as on AWS SQS. */
export const queueNameSchema = z
  .string()
  .min(1)
  .max(80)
  .regex(NAME_PATTERN, NAME_MESSAGE);

/** A new namespace's name. */
export const namespaceNameSchema = z
  .string()
  .min(1)
  .max(32)
  .regex(NAME_PATTERN, NAME_MESSAGE);
