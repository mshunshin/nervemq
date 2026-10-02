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

import { useMutation, useQuery } from "@tanstack/react-query";
import { deleteUserApiKey, listUserApiKeys } from "@/lib/actions/api";
import { Trash2 } from "lucide-react";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import { Spinner } from "./ui/spinner";
import { keyAccessLabel } from "@/lib/key-access";

/** An admin's view of another user's API keys, with revoke. */
export default function UserApiKeys({
  email,
  close,
}: {
  email?: string;
  close: () => void;
}) {
  const { data: keys = [], isLoading } = useQuery({
    queryKey: ["user-api-keys", { email }],
    queryFn: () => listUserApiKeys(email ?? ""),
    enabled: email !== undefined,
  });

  const invalidateUserKeys = useInvalidate(["user-api-keys"]);
  const invalidateOwnKeys = useInvalidate(["apiKeys"]);

  const { mutate: revoke, isPending } = useMutation({
    mutationFn: deleteUserApiKey,
    onSuccess: (_, { name }) => {
      invalidateUserKeys();
      invalidateOwnKeys();
      toast.success(`API key ${name} revoked`);
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to revoke API key"),
  });

  return (
    <Dialog open={email !== undefined} onOpenChange={(open) => !open && close()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>API Keys</DialogTitle>
          <DialogDescription>
            Keys belonging to {email}. Revoking one stops it authenticating
            immediately.
          </DialogDescription>
        </DialogHeader>
        {isLoading ? (
          <div className="flex justify-center py-4">
            <Spinner size="sm" />
          </div>
        ) : keys.length === 0 ? (
          <p className="text-sm text-muted-foreground">No API keys.</p>
        ) : (
          <ul className="flex flex-col divide-y">
            {keys.map((key) => (
              <li
                key={key.name}
                className="flex items-center justify-between gap-2 py-2"
              >
                <div className="min-w-0">
                  <p className="truncate font-medium">{key.name}</p>
                  <p className="truncate text-sm text-muted-foreground">
                    {key.namespace} · {keyAccessLabel(key.access)} access
                  </p>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  className="text-destructive hover:text-destructive hover:bg-destructive/10"
                  disabled={isPending}
                  title={`Revoke ${key.name}`}
                  onClick={() =>
                    email && revoke({ email, name: key.name })
                  }
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </li>
            ))}
          </ul>
        )}
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="secondary">Close</Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
