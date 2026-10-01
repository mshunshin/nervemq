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
} from "./ui/dialog";

import { useMutation } from "@tanstack/react-query";
import { resetUserPassword } from "@/lib/actions/api";
import { toast } from "sonner";
import { useState } from "react";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { passwordSchema } from "@/lib/schemas/password";

/** Lets an admin set another user's password without knowing the old one. */
export default function ResetPassword({
  email,
  close,
}: {
  email?: string;
  close: () => void;
}) {
  const [password, setPassword] = useState("");

  const { mutate: doReset, isPending } = useMutation({
    mutationFn: resetUserPassword,
    onSuccess: () => {
      toast.success(`Password reset for ${email}`);
      setPassword("");
      close();
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to reset password"),
  });

  const error = password === "" ? undefined : passwordSchema.safeParse(password).error;

  return (
    <Dialog
      open={email !== undefined}
      onOpenChange={(open) => {
        if (!open) {
          setPassword("");
          close();
        }
      }}
    >
      <DialogContent>
        <form
          className="flex flex-col gap-4"
          onSubmit={(e) => {
            e.preventDefault();
            if (email) doReset({ email, password });
          }}
        >
          <DialogHeader>
            <DialogTitle>Reset Password</DialogTitle>
            <DialogDescription>
              Set a new password for {email}. Their API keys are not affected.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-2">
            <Label htmlFor="reset-password">New password</Label>
            <Input
              id="reset-password"
              type="password"
              autoComplete="new-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
            {error ? (
              <span className="text-sm text-destructive">
                {error.issues[0]?.message}
              </span>
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
              disabled={isPending || password === "" || error !== undefined}
            >
              {isPending ? "Saving..." : "Reset Password"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
