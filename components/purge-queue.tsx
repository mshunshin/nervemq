"use client";

import { Button } from "./ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "./ui/dialog";

import { useMutation } from "@tanstack/react-query";
import { purgeQueue } from "@/lib/actions/api";
import { Eraser } from "lucide-react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import { useState } from "react";

/** Confirm-and-purge button: removes every message, keeps the queue. */
export default function PurgeQueue({
  namespace,
  queue,
}: {
  namespace: string;
  queue: string;
}) {
  const [open, setOpen] = useState(false);

  const invalidateMessages = useInvalidate(["queue-messages"]);
  const invalidateQueues = useInvalidate(["queues"]);

  const { mutate: doPurge, isPending } = useMutation({
    mutationFn: purgeQueue,
    onSuccess: () => {
      invalidateMessages();
      invalidateQueues();
      toast.success(`Queue ${queue} purged`);
      setOpen(false);
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to purge queue"),
  });

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Eraser className="h-4 w-4" />
          Purge
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Purge Queue</DialogTitle>
          <DialogDescription>
            Permanently delete every message in {namespace}/{queue}, including
            messages currently in flight? The queue itself and its settings
            are kept. This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter className="gap-2">
          <DialogClose asChild>
            <Button variant="secondary" disabled={isPending}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            variant="destructive"
            disabled={isPending}
            onClick={() => doPurge({ namespace, queue })}
          >
            {isPending ? "Purging..." : "Purge Queue"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
