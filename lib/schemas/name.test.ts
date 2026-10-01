// Run with `bun test` (see lib/key-access.test.ts).
import { describe, expect, test } from "bun:test";
import { namespaceNameSchema, queueNameSchema } from "./name";
import { passwordSchema } from "./password";

const ok = (schema: { safeParse: (v: unknown) => { success: boolean } }, v: string) =>
  schema.safeParse(v).success;

describe("queue names follow AWS's rule", () => {
  test.each(["jobs", "order-events", "order_events", "_", "-", "a".repeat(80)])(
    "accepts %p",
    (name) => expect(ok(queueNameSchema, name)).toBe(true),
  );
  test.each(["", "a b", "a.b", "a/b", "café", "a".repeat(81)])("refuses %p", (name) =>
    expect(ok(queueNameSchema, name)).toBe(false),
  );
});

describe("namespace names", () => {
  test("allow the same alphabet up to 32 characters", () => {
    expect(ok(namespaceNameSchema, "team-a_1")).toBe(true);
    expect(ok(namespaceNameSchema, "n".repeat(32))).toBe(true);
    expect(ok(namespaceNameSchema, "n".repeat(33))).toBe(false);
    expect(ok(namespaceNameSchema, "a.b")).toBe(false);
  });
});

describe("passwords", () => {
  test("are 8 to 32 characters", () => {
    expect(ok(passwordSchema, "1234567")).toBe(false);
    expect(ok(passwordSchema, "12345678")).toBe(true);
    expect(ok(passwordSchema, "x".repeat(32))).toBe(true);
    expect(ok(passwordSchema, "x".repeat(33))).toBe(false);
  });
});
