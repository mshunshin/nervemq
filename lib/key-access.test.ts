// Run with `bun test`. Excluded from tsconfig: bun's own types aren't a
// dependency, and Next's type check would trip on `bun:test`.
import { describe, expect, test } from "bun:test";
import {
  KEY_ACCESS_LEVELS,
  capKeyAccess,
  keyAccessLabel,
  maxKeyAccess,
} from "./key-access";

describe("maxKeyAccess", () => {
  // The levels a caller may give a key in a namespace — the server enforces
  // the same rule; this decides which options the dialog greys out.
  test.each([
    [true, true, "admin"],
    [true, false, "admin"],
    [false, true, "owner"],
    [false, false, "member"],
  ] as const)("admin=%p owner=%p -> %p", (isAdmin, canManage, expected) => {
    expect(maxKeyAccess(isAdmin, canManage)).toBe(expected);
  });
});

describe("capKeyAccess", () => {
  test("never raises a level", () => {
    expect(capKeyAccess("member", "admin")).toBe("member");
    expect(capKeyAccess("owner", "admin")).toBe("owner");
  });

  test("lowers a level above the maximum", () => {
    expect(capKeyAccess("admin", "owner")).toBe("owner");
    expect(capKeyAccess("admin", "member")).toBe("member");
    expect(capKeyAccess("owner", "member")).toBe("member");
  });
});

test("levels are listed from most to least access, with labels", () => {
  expect(KEY_ACCESS_LEVELS.map((l) => l.value)).toEqual([
    "admin",
    "owner",
    "member",
  ]);
  expect(keyAccessLabel("owner")).toBe("Owner");
});
