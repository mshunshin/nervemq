import { Button } from "./ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";

import { useMutation } from "@tanstack/react-query";
import { changeOwnPassword } from "@/lib/actions/api";
import { toast } from "sonner";
import { useState } from "react";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { passwordSchema } from "@/lib/schemas/password";

/** Lets the logged-in user change their own password. */
export default function ChangePassword({
  open,
  close,
}: {
  open: boolean;
  close: () => void;
}) {
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");

  const reset = () => {
    setCurrent("");
    setNext("");
    setConfirm("");
  };

  const { mutate: doChange, isPending } = useMutation({
    mutationFn: changeOwnPassword,
    onSuccess: () => {
      toast.success("Password changed");
      reset();
      close();
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to change password"),
  });

  const error =
    next === ""
      ? undefined
      : (passwordSchema.safeParse(next).error?.issues[0]?.message ??
        (confirm !== "" && confirm !== next
          ? "Passwords do not match"
          : undefined));

  return (
    <Dialog
      open={open}
      onOpenChange={(open) => {
        if (!open) {
          reset();
          close();
        }
      }}
    >
      <DialogContent>
        <form
          className="flex flex-col gap-4"
          onSubmit={(e) => {
            e.preventDefault();
            doChange({ currentPassword: current, newPassword: next });
          }}
        >
          <DialogHeader>
            <DialogTitle>Change Password</DialogTitle>
            <DialogDescription>
              Your API keys are not affected.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-2">
            <Label htmlFor="current-password">Current password</Label>
            <Input
              id="current-password"
              type="password"
              autoComplete="current-password"
              value={current}
              onChange={(e) => setCurrent(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="new-password">New password</Label>
            <Input
              id="new-password"
              type="password"
              autoComplete="new-password"
              value={next}
              onChange={(e) => setNext(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="confirm-password">Confirm new password</Label>
            <Input
              id="confirm-password"
              type="password"
              autoComplete="new-password"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
            />
            {error ? (
              <span className="text-sm text-destructive">{error}</span>
            ) : null}
          </div>
          <DialogFooter className="gap-2">
            <DialogClose asChild>
              <Button variant="secondary" type="button" disabled={isPending}>
                Cancel
              </Button>
            </DialogClose>
            <Button
              type="submit"
              disabled={
                isPending ||
                current === "" ||
                next === "" ||
                next !== confirm ||
                error !== undefined
              }
            >
              {isPending ? "Saving..." : "Change Password"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
