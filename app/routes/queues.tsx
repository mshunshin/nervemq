import { listNamespaces, listQueues } from "@/lib/actions/api";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router";

import { columns } from "@/components/queues/table";
import type { QueueStatistics } from "@/lib/types";
import { DataTable } from "@/components/data-table";
import CreateQueue from "@/components/create-queue";
import NotFound from "@/components/not-found";
import { Button } from "@/components/ui/button";
import { useMemo, useState } from "react";
import { deleteQueue } from "@/lib/actions/api";
import { useNamespaceAccess } from "@/lib/hooks/use-namespace-access";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { SortingState } from "@tanstack/react-table";
import { Input } from "@/components/ui/input";
import { deleteQueueSchema } from "@/lib/schemas/delete-queue";
import { toast } from "sonner";

/** The picker's value for every namespace; no namespace name contains `*`. */
const ALL_NAMESPACES = "*";

/**
 * The queue list: every queue the user can access at /queues, or one
 * namespace's at /queues/:namespace (where the queue page's breadcrumb
 * links).
 */
export default function Queues() {
  const { namespace } = useParams();
  const { canManage } = useNamespaceAccess();
  const [isOpen, setIsOpen] = useState(false);
  const navigate = useNavigate();
  const [queueToDelete, setQueueToDelete] = useState<{
    name: string;
    ns: string;
  } | null>(null);
  const [sorting, setSorting] = useState<SortingState>([]);
  const [searchQuery, setSearchQuery] = useState("");

  // The namespaces the user can access, for the picker, and to tell a
  // namespace that doesn't exist (or isn't theirs) from an empty one.
  const { data: namespaces } = useQuery({
    queryKey: ["namespaces"],
    queryFn: () => listNamespaces(),
  });

  const {
    data = [],
    isLoading,
    refetch,
  } = useQuery({
    queryFn: () => listQueues(),
    queryKey: ["queues"],
    select: (data) =>
      Array.from(data.values()).filter(
        (queue: QueueStatistics) =>
          (namespace === undefined || queue.ns === namespace) &&
          queue.name.toLowerCase().includes(searchQuery.toLowerCase()),
      ),
  });

  // Within one namespace, its column would repeat the same name on every row.
  const visibleColumns = useMemo(
    () =>
      namespace === undefined
        ? columns
        : columns.filter((column) => column.id !== "ns"),
    [namespace],
  );

  const { mutate: removeQueue, isPending: isDeleting } = useMutation({
    mutationFn: deleteQueue,
    onSuccess: () => {
      refetch();
      setQueueToDelete(null);
    },
    onError: () => toast.error("Something went wrong"),
  });

  const handleDeleteQueue = async (
    name: string,
    ns: string,
    e: React.MouseEvent,
  ) => {
    e.stopPropagation();
    setQueueToDelete({ name, ns });
  };

  if (
    namespace !== undefined &&
    namespaces !== undefined &&
    !namespaces.some((ns) => ns.name === namespace)
  ) {
    return (
      <NotFound
        resource="namespace"
        returnTo={{ name: "Queues", href: "/queues" }}
      />
    );
  }

  return (
    <div className="h-full flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <div className="flex w-full flex-wrap items-center gap-2">
          <Select
            value={namespace ?? ALL_NAMESPACES}
            onValueChange={(value) =>
              navigate(
                value === ALL_NAMESPACES
                  ? "/queues"
                  : `/queues/${encodeURIComponent(value)}`,
              )
            }
          >
            <SelectTrigger className="w-56" aria-label="Namespace">
              {/* Named outright, so it reads right before the list loads. */}
              <SelectValue>{namespace ?? "All namespaces"}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL_NAMESPACES}>All namespaces</SelectItem>
              {namespaces !== undefined && namespaces.length > 0 ? (
                <SelectSeparator />
              ) : null}
              {(namespaces ?? []).map((ns) => (
                <SelectItem key={ns.name} value={ns.name}>
                  {ns.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            className="max-w-sm"
            type="text"
            placeholder="Search queues..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
        <p className="text-sm text-muted-foreground">
          {namespace === undefined ? (
            "Queues in every namespace you can access."
          ) : (
            <>
              Queues in the{" "}
              <span className="font-medium text-foreground">{namespace}</span>{" "}
              namespace only.{" "}
              <Link to="/queues" className="underline underline-offset-4">
                Show all namespaces
              </Link>
            </>
          )}
        </p>
      </div>

      <DataTable
        className="w-full"
        columns={visibleColumns}
        data={data}
        isLoading={isLoading}
        onRowClick={(row: QueueStatistics) =>
          navigate(
            `/queues/${encodeURIComponent(row.ns)}/${encodeURIComponent(row.name)}`,
          )
        }
        meta={{ handleDeleteQueue, canManage }}
        sorting={sorting}
        setSorting={setSorting}
      />

      <div className="flex justify-end">
        <Button onClick={() => setIsOpen(true)}>Create Queue</Button>
      </div>
      <CreateQueue
        open={isOpen}
        close={() => setIsOpen(false)}
        namespace={namespace}
      />

      <Dialog
        open={!!queueToDelete}
        onOpenChange={(open) => !open && setQueueToDelete(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Queue</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this queue? This action cannot be
              undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="destructive"
              disabled={isDeleting}
              onClick={() => {
                if (queueToDelete) {
                  // The schema field is `namespace`; the table row carries
                  // `ns`. The old code parsed {name, ns} directly, which
                  // always failed validation — deletes from this page never
                  // reached the server.
                  const req = deleteQueueSchema.safeParse({
                    name: queueToDelete.name,
                    namespace: queueToDelete.ns,
                  });
                  if (req.success) {
                    removeQueue(req.data);
                  } else {
                    toast.error("Invalid queue name");
                  }
                }
              }}
            >
              Delete
            </Button>
            <Button variant="secondary" onClick={() => setQueueToDelete(null)}>
              Cancel
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
