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
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Input } from "./ui/input";
import { Label } from "./ui/label";

import { useMutation, useQuery } from "@tanstack/react-query";
import { getQueueAttributes, setQueueAttributes } from "@/lib/actions/api";
import { Spinner } from "@/components/ui/spinner";
import { Pencil } from "lucide-react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import { useState } from "react";
import {
  STANDARD_QUEUE_ATTRIBUTES,
  type StandardQueueAttribute,
} from "@/lib/types";

/**
 * Shows the queue's standard SQS attributes (visibility timeout, delivery
 * delay, …) and lets them be edited. Values are positive integers carried
 * as strings, AWS-style; unset attributes fall back to server defaults.
 */
export default function QueueAttributesCard({
  namespace,
  queue,
}: {
  namespace: string;
  queue: string;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<
    Partial<Record<StandardQueueAttribute, string>>
  >({});

  const { data: attributes, isLoading } = useQuery({
    queryKey: ["queueAttributes", namespace, queue],
    queryFn: () => getQueueAttributes(namespace, queue),
  });

  const invalidate = useInvalidate(["queueAttributes"]);

  const { mutate: doSave, isPending } = useMutation({
    mutationFn: setQueueAttributes,
    onSuccess: () => {
      invalidate();
      toast.success("Queue attributes updated");
      setOpen(false);
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to update attributes"),
  });

  const openEditor = () => {
    // Prefill the form with the current values; untouched/empty fields are
    // not sent (the API upserts only the attributes present in the request).
    const current: Partial<Record<StandardQueueAttribute, string>> = {};
    for (const [key] of STANDARD_QUEUE_ATTRIBUTES) {
      const value = attributes?.[key];
      if (value !== undefined) {
        current[key] = value;
      }
    }
    setDraft(current);
    setOpen(true);
  };

  const invalidDraft = Object.values(draft).some(
    (value) => value !== "" && !/^\d+$/.test(value ?? ""),
  );

  const save = () => {
    const changed: Partial<Record<StandardQueueAttribute, string>> = {};
    for (const [key] of STANDARD_QUEUE_ATTRIBUTES) {
      const value = draft[key];
      if (value !== undefined && value !== "" && value !== attributes?.[key]) {
        changed[key] = value;
      }
    }
    if (Object.keys(changed).length === 0) {
      setOpen(false);
      return;
    }
    doSave({ namespace, queue, attributes: changed });
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle>Attributes</CardTitle>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button
              size="icon"
              variant="ghost"
              onClick={(e) => {
                e.preventDefault();
                openEditor();
              }}
              disabled={isLoading}
            >
              <Pencil className="h-4 w-4" />
            </Button>
          </DialogTrigger>
          <DialogContent className="rounded-lg sm:rounded-lg">
            <DialogHeader>
              <DialogTitle>Edit Queue Attributes</DialogTitle>
              <DialogDescription>
                Standard SQS attributes for {namespace}/{queue}. Leave a field
                empty to keep the server default.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-4 py-2">
              {STANDARD_QUEUE_ATTRIBUTES.map(([key, label]) => (
                <div key={key} className="flex flex-col gap-2">
                  <Label htmlFor={key}>{label}</Label>
                  <Input
                    id={key}
                    type="number"
                    min={0}
                    value={draft[key] ?? ""}
                    placeholder="default"
                    onChange={(e) =>
                      setDraft((prev) => ({ ...prev, [key]: e.target.value }))
                    }
                  />
                </div>
              ))}
              {invalidDraft ? (
                <span className="text-sm text-destructive">
                  Attribute values must be non-negative integers
                </span>
              ) : null}
            </div>
            <DialogFooter className="gap-2">
              <DialogClose asChild>
                <Button variant="secondary" disabled={isPending}>
                  Cancel
                </Button>
              </DialogClose>
              <Button onClick={save} disabled={isPending || invalidDraft}>
                {isPending ? "Saving..." : "Save Changes"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center justify-center min-h-16">
            <Spinner size="sm" />
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {STANDARD_QUEUE_ATTRIBUTES.map(([key, label]) => (
              <div key={key}>
                <p className="text-gray-600 wrap-break-word">{label}</p>
                <p className="text-2xl font-medium">
                  {attributes?.[key] ?? (
                    <span className="text-gray-400">default</span>
                  )}
                </p>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
