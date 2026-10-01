"use client";
import type { ColumnDef } from "@tanstack/react-table";
import {
  KeySquare,
  Logs,
  Trash2,
  ArrowUpDown,
  Crown,
  UserPlus,
} from "lucide-react";
import { Button } from "../ui/button";
import type { NamespaceStatistics } from "@/lib/types";

export const columns: ColumnDef<NamespaceStatistics>[] = [
  {
    accessorKey: "name",
    header: ({ column }) => (
      <div className="flex items-center gap-2">
        <KeySquare className="h-4 w-4" />
        <Button
          variant="ghost"
          className="p-0 hover:bg-transparent"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          <span>Name</span>
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      </div>
    ),
    enableSorting: true,
  },
  {
    // Wire field is snake_case; the previous "queueCount" accessor never
    // matched, so this column always rendered blank.
    accessorKey: "queue_count",
    header: () => (
      <div className="flex items-center gap-2">
        <Logs className="h-4 w-4" />
        <span>Queues</span>
      </div>
    ),
  },
  {
    id: "owners",
    accessorFn: (ns) => ns.owners.join(", "),
    header: () => (
      <div className="flex items-center gap-2">
        <Crown className="h-4 w-4" />
        <span>Owners</span>
      </div>
    ),
    cell: ({ row }) =>
      row.original.owners.length === 0 ? (
        <span className="text-muted-foreground">—</span>
      ) : (
        <span>{row.original.owners.join(", ")}</span>
      ),
  },
  {
    id: "created_by",
    accessorFn: (ns) => ns.created_by ?? "",
    header: () => (
      <div className="flex items-center gap-2">
        <UserPlus className="h-4 w-4" />
        <span>Created by</span>
      </div>
    ),
    cell: ({ row }) =>
      row.original.created_by ?? (
        <span className="text-muted-foreground">—</span>
      ),
  },
  {
    id: "actions",
    cell: (row) =>
      // Admins and owners may delete; the server refuses anyone else.
      row.row.original.can_manage ? (
        <div className="flex items-center justify-end gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="text-destructive hover:text-destructive hover:bg-destructive/10"
            onClick={(e) => {
              const meta = row.table.options.meta as
                | {
                    handleDeleteNamespace: (name: string, e: unknown) => void;
                  }
                | undefined;
              meta?.handleDeleteNamespace(row.row.original.name, e);
            }}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      ) : null,
  },
];
