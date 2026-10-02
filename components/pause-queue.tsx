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
import { setQueuePaused } from "@/lib/actions/api";
import type { QueueStatistics } from "@/lib/types";
import { Pause, Play } from "lucide-react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import { useState } from "react";

/**
 * Pause/resume button. Pausing stops every consumer, so it asks first;
 * resuming does not.
 */
export default function PauseQueue({ queue }: { queue?: QueueStatistics }) {
  const [open, setOpen] = useState(false);

  const invalidateQueues = useInvalidate(["queues"]);

  const { mutate: setPaused, isPending } = useMutation({
    mutationFn: setQueuePaused,
    onSuccess: (_, { queue: name, paused }) => {
      invalidateQueues();
      toast.success(`Queue ${name} ${paused ? "paused" : "resumed"}`);
      setOpen(false);
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to change the queue's state"),
  });

  if (queue === undefined) {
    return null;
  }

  const target = { namespace: queue.ns, queue: queue.name };

  if (queue.paused_at !== null) {
    return (
      <Button
        variant="outline"
        size="sm"
        disabled={isPending}
        onClick={() => setPaused({ ...target, paused: false })}
      >
        <Play className="h-4 w-4" />
        {isPending ? "Resuming..." : "Resume"}
      </Button>
    );
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Pause className="h-4 w-4" />
          Pause
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Pause Queue</DialogTitle>
          <DialogDescription>
            Consumers of {queue.ns}/{queue.name} will receive no messages until
            you resume it. Sending, deleting and changing visibility keep
            working, so consumers can finish the messages they hold. Once no
            messages are in flight, the consumers can be swapped.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter className="gap-2">
          <DialogClose asChild>
            <Button variant="secondary" disabled={isPending}>
              Cancel
            </Button>
          </DialogClose>
          <Button
            disabled={isPending}
            onClick={() => setPaused({ ...target, paused: true })}
          >
            {isPending ? "Pausing..." : "Pause Queue"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
