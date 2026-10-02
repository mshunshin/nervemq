import type { ColumnDef } from "@tanstack/react-table";
import { Activity, Braces, KeySquare, Trash2, ArrowUpDown } from "lucide-react";
import { Button } from "../ui/button";
import type { QueueStatistics } from "@/lib/types";

export const columns: ColumnDef<QueueStatistics>[] = [
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
    // Named so the single-namespace view (/queues/:namespace) can drop it.
    id: "ns",
    accessorKey: "ns",
    header: ({ column }) => (
      <div className="flex items-center gap-2">
        <Braces className="h-4 w-4" />
        <Button
          variant="ghost"
          className="p-0 hover:bg-transparent"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          <span>Namespace</span>
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      </div>
    ),
    enableSorting: true,
  },
  {
    id: "status",
    accessorFn: (queue) => (queue.paused_at !== null ? "paused" : "running"),
    header: () => (
      <div className="flex items-center gap-2">
        <Activity className="h-4 w-4" />
        <span>Status</span>
      </div>
    ),
    cell: ({ row }) =>
      row.original.paused_at !== null ? (
        <span className="text-destructive">Paused</span>
      ) : (
        <span className="text-muted-foreground">Running</span>
      ),
  },
  {
    id: "actions",
    cell: (row) => {
      const meta = row.table.options.meta as
        | {
            handleDeleteQueue: (name: string, ns: string, e: unknown) => void;
            canManage: (namespace: string) => boolean;
          }
        | undefined;
      // Only admins and the namespace's owners may delete its queues.
      if (!meta?.canManage(row.row.original.ns)) {
        return null;
      }
      return (
        <div className="flex items-center justify-end gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="text-destructive hover:text-destructive hover:bg-destructive/10"
            onClick={async (e) => {
              meta.handleDeleteQueue(
                row.row.original.name,
                row.row.original.ns,
                e,
              );
            }}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      );
    },
  },
];
