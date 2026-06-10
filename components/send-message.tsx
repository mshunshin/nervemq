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

import { useForm } from "@tanstack/react-form";
import { useMutation } from "@tanstack/react-query";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";
import { sendQueueMessage } from "@/lib/actions/api";
import { Spinner } from "@/components/ui/spinner";
import { Plus } from "lucide-react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import { useState } from "react";
import {
  type SendMessageForm,
  sendMessageSchema,
} from "@/lib/schemas/send-message";
import KeyValueForm from "./key-value-pairs";

/** Dialog (with its own trigger button) for enqueuing a message. */
export default function SendMessage({
  namespace,
  queue,
}: {
  namespace: string;
  queue: string;
}) {
  const [open, setOpen] = useState(false);

  const invalidateMessages = useInvalidate(["queue-messages"]);
  const invalidateQueues = useInvalidate(["queues"]);

  const { mutateAsync: doSend } = useMutation({
    mutationFn: sendQueueMessage,
    onSuccess: () => {
      invalidateMessages();
      invalidateQueues();
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to send message"),
  });

  const form = useForm({
    defaultValues: {
      body: "",
      attributes: new Map(),
    } as SendMessageForm,
    validators: {
      onChange: sendMessageSchema,
      onMount: sendMessageSchema,
      onSubmit: sendMessageSchema,
    },
    onSubmit: async ({ value: data, formApi }) => {
      let result: { MessageId: string };
      try {
        result = await doSend({
          namespace,
          queue,
          body: data.body,
          attributes: data.attributes,
        });
      } catch {
        // Error toast handled by the mutation's onError.
        return;
      }
      toast.success(`Message ${result.MessageId} sent`);
      setOpen(false);
      formApi.reset();
    },
  });

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Plus className="h-4 w-4" />
          Send Message
        </Button>
      </DialogTrigger>
      <DialogContent className="rounded-lg sm:rounded-lg">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            e.stopPropagation();
            void form.handleSubmit();
          }}
          className="flex flex-col gap-4"
        >
          <DialogHeader>
            <DialogTitle>Send Message</DialogTitle>
            <DialogDescription>
              Enqueue a message on {namespace}/{queue}. Queue delivery-delay
              and size limits apply.
            </DialogDescription>
          </DialogHeader>
          <form.Field name="body">
            {(field) => (
              <div className="flex flex-col gap-2">
                <Label htmlFor={field.name}>Message Body</Label>
                <Textarea
                  id={field.name}
                  name={field.name}
                  value={field.state.value}
                  onBlur={field.handleBlur}
                  onChange={(e) => field.handleChange(e.target.value)}
                  placeholder="Message body"
                  rows={5}
                />
                {field.state.meta.errors.length > 0 ? (
                  <span className="text-sm text-destructive">
                    {field.state.meta.errors.map((e) => e?.message).join(", ")}
                  </span>
                ) : null}
              </div>
            )}
          </form.Field>

          <form.Field name="attributes">
            {(field) => (
              <div className="flex flex-col gap-2">
                <Label htmlFor={field.name}>Message Attributes</Label>
                <KeyValueForm
                  value={field.state.value}
                  onChange={(value) => field.handleChange(value)}
                />
              </div>
            )}
          </form.Field>

          <DialogFooter>
            <form.Subscribe
              selector={(state) => [state.canSubmit, state.isSubmitting]}
            >
              {([canSubmit, isSubmitting]) => (
                <div className="flex flex-col sm:flex-row gap-2">
                  <Button type="submit" disabled={!canSubmit}>
                    {isSubmitting ? (
                      <>
                        <Spinner className="absolute self-center" size="sm" />
                        <p className="text-transparent">Send</p>
                      </>
                    ) : (
                      "Send"
                    )}
                  </Button>

                  <DialogClose asChild>
                    <Button variant="secondary" disabled={isSubmitting}>
                      Cancel
                    </Button>
                  </DialogClose>
                </div>
              )}
            </form.Subscribe>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
