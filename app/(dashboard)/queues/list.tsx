"use client";

import { DataTable } from "@/components/data-table";
import type {
  Column,
  ColumnDef,
  ColumnFiltersState,
  SortingState,
} from "@tanstack/react-table";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  MoreHorizontal,
  RotateCcw,
  Trash2,
  XCircle,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  keepPreviousData,
  useMutation,
  useQuery,
} from "@tanstack/react-query";
import {
  clearFailedMessages,
  deleteQueueMessage,
  listMessages,
  type MessageSortKey,
  updateMessageStatus,
} from "@/lib/actions/api";
import { Filter, Check } from "lucide-react";
import {
  Popover,
  PopoverTrigger,
  PopoverContent,
} from "@/components/ui/popover";
import {
  Command,
  CommandInput,
  CommandItem,
  CommandList,
  CommandEmpty,
  CommandGroup,
} from "@/components/ui/command";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import React from "react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import type { MessageObject, SettableMessageStatus } from "@/lib/types";

/** A column header that toggles none → asc → desc server-side sorting. */
function SortableHeader({
  column,
  children,
}: {
  column: Column<MessageObject, unknown>;
  children: React.ReactNode;
}) {
  const sorted = column.getIsSorted();
  return (
    <Button
      variant="ghost"
      className="p-0 hover:bg-transparent"
      onClick={() => column.toggleSorting(sorted === "asc")}
    >
      {children}
      {sorted === "asc" ? (
        <ArrowUp className="ml-1 h-4 w-4" />
      ) : sorted === "desc" ? (
        <ArrowDown className="ml-1 h-4 w-4" />
      ) : (
        <ArrowUpDown className="ml-1 h-4 w-4 opacity-40" />
      )}
    </Button>
  );
}

function MessageDetails({ message }: { message: MessageObject }) {
  return (
    <div className="p-6 space-y-4 bg-gray-50">
      <h3 className="font-semibold text-gray-700 mb-2">Message Details</h3>
      {/* Timestamps Section */}
      <div className="bg-white p-4 rounded-lg border border-gray-200">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <span className="text-xs uppercase text-gray-400">Received</span>
            <div className="mt-1 text-sm text-gray-700">
              {message.received_at === null
                ? "—"
                : new Date(message.received_at * 1000).toLocaleString()}
            </div>
          </div>
          <div>
            <span className="text-xs uppercase text-gray-400">
              Last Delivered
            </span>
            <div className="mt-1 text-sm text-gray-700">
              {message.delivered_at === null
                ? "—"
                : new Date(message.delivered_at * 1000).toLocaleString()}
            </div>
          </div>
        </div>
      </div>
      {/* Message Body Section */}
      <div className="bg-white p-4 rounded-lg border border-gray-200">
        <span className="text-xs uppercase text-gray-400">Message Body</span>
        <div className="mt-1 text-sm text-gray-700 whitespace-pre-wrap">
          {message.body}
        </div>
      </div>

      {/* Existing Key-Value Pairs Section */}
      {Object.entries(message.message_attributes).length === 0 ? (
        <div className="bg-white p-4 rounded-lg border border-gray-200 text-gray-500 text-sm">
          No message details available
        </div>
      ) : (
        <div className="grid gap-3">
          {Object.entries(message.message_attributes)?.map(([k, v], index) => (
            <div
              key={`message-${index.toString()}`}
              className="bg-white p-4 rounded-lg border border-gray-200"
            >
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <span className="text-xs uppercase text-gray-400">Key</span>
                  <div className="mt-1 text-sm font-medium text-gray-700">
                    {k}
                  </div>
                </div>
                <div>
                  <span className="text-xs uppercase text-gray-400">Value</span>
                  <div className="mt-1 text-sm text-gray-700">{v}</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function MessageList({
  queue,
  namespace,
}: {
  queue?: string;
  namespace?: string;
}) {
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>(
    [],
  );

  // Server-side pagination: each page is one request, so the status filter
  // in the header only narrows the rows of the current page.
  const [pagination, setPagination] = React.useState({
    pageIndex: 0,
    pageSize: 50,
  });
  const { pageIndex, pageSize } = pagination;

  // Server-side sorting: the header click updates this, which re-keys the
  // query; the column ids match the server's sort-key whitelist.
  const [sorting, setSorting] = React.useState<SortingState>([]);
  const sortKey = (sorting[0]?.id as MessageSortKey | undefined) ?? "id";
  const sortOrder = sorting[0]?.desc ? "desc" : "asc";

  // A new sort order restarts from the first page.
  const updateSorting = React.useCallback((next: SortingState) => {
    setSorting(next);
    setPagination((p) => ({ ...p, pageIndex: 0 }));
  }, []);

  const { data, isLoading } = useQuery({
    queryKey: [
      "queue-messages",
      { queue, namespace, pageIndex, pageSize, sortKey, sortOrder },
    ],
    queryFn: () => {
      if (queue === undefined || namespace === undefined) {
        return { messages: [], total: 0 };
      }
      return listMessages({
        queue,
        namespace,
        limit: pageSize,
        offset: pageIndex * pageSize,
        sort: sortKey,
        order: sortOrder,
      });
    },
    // Keep the previous page on screen while the next one loads.
    placeholderData: keepPreviousData,
  });

  const messages = data?.messages ?? [];
  const total = data?.total ?? 0;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));

  // If the queue shrank past the current page (deletes, purge), snap back to
  // the last page that still exists.
  React.useEffect(() => {
    if (pageIndex > 0 && pageIndex >= pageCount) {
      // The valid page range is derived from a server response, which can't
      // be known during render.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setPagination((p) => ({ ...p, pageIndex: pageCount - 1 }));
    }
  }, [pageIndex, pageCount]);

  const invalidateMessages = useInvalidate(["queue-messages"]);
  const invalidateQueues = useInvalidate(["queues"]);
  const refresh = React.useCallback(() => {
    invalidateMessages();
    invalidateQueues();
    // The invalidate callbacks are stable closures over the query client.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const { mutate: removeMessage, isPending: isDeleting } = useMutation({
    mutationFn: deleteQueueMessage,
    onSuccess: () => {
      refresh();
      toast.success("Message deleted");
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to delete message"),
  });

  const { mutate: setStatus, isPending: isUpdating } = useMutation({
    mutationFn: updateMessageStatus,
    onSuccess: (_data, variables) => {
      refresh();
      toast.success(
        variables.status === "pending"
          ? "Message requeued"
          : "Message marked as failed",
      );
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to update message"),
  });

  const { mutate: clearFailed, isPending: isClearing } = useMutation({
    mutationFn: clearFailedMessages,
    onSuccess: ({ deleted }) => {
      refresh();
      toast.success(
        deleted === 0
          ? "No failed messages to clear"
          : `Cleared ${deleted} failed message${deleted === 1 ? "" : "s"}`,
      );
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to clear failed messages"),
  });

  // Columns close over the queue identity and the mutations, so they live
  // inside the component.
  const columns = React.useMemo<ColumnDef<MessageObject>[]>(() => {
    const changeStatus = (id: number, status: SettableMessageStatus) => {
      if (queue === undefined || namespace === undefined) return;
      setStatus({ namespace, queue, id, status });
    };

    return [
      {
        id: "expand",
        header: "",
        cell: ({ row }) => {
          return (
            <Button
              onClick={() => row.toggleExpanded()}
              className="p-2 hover:bg-gray-100 rounded bg-transparent w-10"
              variant="ghost"
            >
              {row.getIsExpanded() ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
            </Button>
          );
        },
        enableResizing: false,
        enableHiding: false,
        size: 40,
        minSize: 40,
        maxSize: 40,
      },
      {
        accessorKey: "id",
        header: ({ column }) => (
          <SortableHeader column={column}>ID</SortableHeader>
        ),
      },
      {
        accessorKey: "body",
        header: ({ column }) => (
          <SortableHeader column={column}>Body</SortableHeader>
        ),
        cell: ({ row }) => (
          <span className="block max-w-[260px] truncate text-gray-600">
            {row.original.body}
          </span>
        ),
      },
      {
        accessorKey: "status",
        cell: ({ row }) => {
          return (
            <span
              className={`px-3 py-1 rounded-full text-sm ${
                row.original.status === "delivered"
                  ? "bg-green-100 text-green-800"
                  : row.original.status === "failed"
                    ? "bg-red-100 text-red-800"
                    : "bg-yellow-100 text-yellow-800"
              }`}
            >
              {row.original.status}
            </span>
          );
        },
        header: ({ column }) => {
          const selectedStatus = column.getFilterValue() as string;
          const statuses = ["delivered", "failed", "pending"];

          return (
            <div className="flex items-center gap-2">
              <SortableHeader column={column}>Status</SortableHeader>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="ghost" className="p-0 hover:bg-transparent">
                    <Filter className="h-4 w-4" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[200px] p-0">
                  <Command>
                    <CommandInput placeholder="Filter status..." />
                    <CommandList>
                      <CommandEmpty>No status found</CommandEmpty>
                      <CommandGroup>
                        {statuses.map((status) => (
                          <CommandItem
                            key={status}
                            value={status}
                            onSelect={(value) => {
                              column.setFilterValue(
                                value === selectedStatus ? undefined : value,
                              );
                            }}
                          >
                            <Check
                              className={`mr-2 h-4 w-4 ${
                                selectedStatus === status
                                  ? "opacity-100"
                                  : "opacity-0"
                              }`}
                            />
                            {status}
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    </CommandList>
                  </Command>
                </PopoverContent>
              </Popover>
            </div>
          );
        },
      },
      {
        accessorKey: "tries",
        header: ({ column }) => (
          <SortableHeader column={column}>Retries</SortableHeader>
        ),
      },
      {
        accessorKey: "received_at",
        header: ({ column }) => (
          <SortableHeader column={column}>Received</SortableHeader>
        ),
        cell: ({ row }) =>
          row.original.received_at === null ? (
            <span className="text-gray-400">—</span>
          ) : (
            <span className="text-gray-600">
              {new Date(row.original.received_at * 1000).toLocaleString()}
            </span>
          ),
      },
      {
        accessorKey: "delivered_at",
        header: ({ column }) => (
          <SortableHeader column={column}>Delivered</SortableHeader>
        ),
        cell: ({ row }) =>
          row.original.delivered_at === null ? (
            <span className="text-gray-400">—</span>
          ) : (
            <span className="text-gray-600">
              {new Date(row.original.delivered_at * 1000).toLocaleString()}
            </span>
          ),
      },
      {
        id: "attributes",
        header: "Attributes",
        cell: ({ row }) => {
          const keys = Object.keys(row.original.message_attributes);
          return keys.length === 0 ? (
            <span className="text-gray-400">—</span>
          ) : (
            <span
              className="block max-w-[180px] truncate text-gray-600"
              title={keys.join(", ")}
            >
              {keys.join(", ")}
            </span>
          );
        },
      },
      {
        id: "actions",
        header: "",
        size: 40,
        cell: ({ row }) => (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                disabled={isDeleting || isUpdating}
              >
                <MoreHorizontal className="h-4 w-4" />
                <span className="sr-only">Message actions</span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>Message {row.original.id}</DropdownMenuLabel>
              <DropdownMenuItem
                disabled={row.original.status === "pending"}
                onClick={() => changeStatus(row.original.id, "pending")}
              >
                <RotateCcw className="mr-2 h-4 w-4" />
                Requeue (mark pending)
              </DropdownMenuItem>
              <DropdownMenuItem
                disabled={row.original.status === "failed"}
                onClick={() => changeStatus(row.original.id, "failed")}
              >
                <XCircle className="mr-2 h-4 w-4" />
                Mark failed
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                className="text-destructive focus:text-destructive"
                onClick={() => {
                  if (queue === undefined || namespace === undefined) return;
                  removeMessage({ namespace, queue, id: row.original.id });
                }}
              >
                <Trash2 className="mr-2 h-4 w-4" />
                Delete
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        ),
      },
    ];
  }, [queue, namespace, setStatus, removeMessage, isDeleting, isUpdating]);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-end">
        <Button
          variant="outline"
          size="sm"
          disabled={isClearing || queue === undefined || namespace === undefined}
          onClick={() => {
            if (queue === undefined || namespace === undefined) return;
            clearFailed({ namespace, queue });
          }}
        >
          <XCircle className="mr-2 h-4 w-4" />
          Clear failed messages
        </Button>
      </div>
      <DataTable
        columns={columns}
        isLoading={isLoading}
        data={messages}
        renderSubComponent={({ row }) => (
          <MessageDetails message={row.original} />
        )}
        sorting={sorting}
        setSorting={updateSorting}
        manualSorting
        columnFilters={columnFilters}
        setColumnFilters={setColumnFilters}
      />
      <div className="flex flex-wrap items-center justify-between gap-2 text-sm text-gray-600">
        <span>
          {total === 0
            ? "No messages"
            : `Showing ${pageIndex * pageSize + 1}–${Math.min(
                (pageIndex + 1) * pageSize,
                total,
              )} of ${total}`}
        </span>
        <div className="flex items-center gap-2">
          <span>Rows per page</span>
          <Select
            value={String(pageSize)}
            onValueChange={(value) =>
              setPagination({ pageIndex: 0, pageSize: Number(value) })
            }
          >
            <SelectTrigger className="h-8 w-[80px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {[25, 50, 100, 250].map((size) => (
                <SelectItem key={size} value={String(size)}>
                  {size}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <span className="tabular-nums">
            Page {pageIndex + 1} of {pageCount}
          </span>
          <Button
            variant="outline"
            size="icon"
            className="h-8 w-8"
            disabled={pageIndex === 0}
            onClick={() =>
              setPagination((p) => ({ ...p, pageIndex: p.pageIndex - 1 }))
            }
          >
            <ChevronLeft className="h-4 w-4" />
            <span className="sr-only">Previous page</span>
          </Button>
          <Button
            variant="outline"
            size="icon"
            className="h-8 w-8"
            disabled={pageIndex + 1 >= pageCount}
            onClick={() =>
              setPagination((p) => ({ ...p, pageIndex: p.pageIndex + 1 }))
            }
          >
            <ChevronRight className="h-4 w-4" />
            <span className="sr-only">Next page</span>
          </Button>
        </div>
      </div>
    </div>
  );
}
