"use client";

import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { DataTable } from "@/components/data-table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import type { UserStatistics } from "@/lib/types";
import CreateUser from "@/components/create-user";
import ModifyUser from "@/components/modify-user";
import UserApiKeys from "@/components/user-api-keys";
import ResetPassword from "@/components/reset-password";
import { columns, type UserTableMeta } from "@/components/admin/table";
import { toast } from "sonner";
import { listUsers, deleteUser, setUserDisabled } from "@/lib/actions/api";
import { useIsAdmin } from "@/lib/state/global";
import { useRouter } from "next/navigation";
import { Input } from "@/components/ui/input";
import type { SortingState } from "@tanstack/react-table";

export default function AdminPanel() {
  const router = useRouter();
  const isAdmin = useIsAdmin();

  // Client-side guard: redirect() during render is a server-component
  // pattern; on the client, navigate from an effect instead. isAdmin is
  // undefined until the session has been verified — only redirect once we
  // know for sure the user isn't an admin.
  useEffect(() => {
    if (isAdmin === false) {
      router.replace("/");
    }
  }, [isAdmin, router]);

  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [userToDelete, setUserToDelete] = useState<string | undefined>(
    undefined,
  );
  const [userToModify, setUserToModify] = useState<UserStatistics | undefined>(
    undefined,
  );
  const [keysOf, setKeysOf] = useState<string | undefined>(undefined);
  const [passwordOf, setPasswordOf] = useState<string | undefined>(undefined);
  const [searchQuery, setSearchQuery] = useState("");
  const [sorting, setSorting] = useState<SortingState>([]);

  const {
    data = [],
    isLoading,
    refetch,
  } = useQuery({
    // Filtering happens client-side in `select`; keeping searchQuery out of
    // the key avoids refetching the user list on every keystroke.
    queryKey: ["users"],
    queryFn: () => listUsers(),
    select: (data) =>
      data.filter((user) =>
        user.email.toLowerCase().includes(searchQuery.toLowerCase()),
      ),
  });

  const { mutate: removeUser, isPending: isDeleting } = useMutation({
    mutationFn: (email: string) => deleteUser({ email }),
    onSuccess: async () => {
      await refetch();
      setUserToDelete(undefined);
      toast.success("User deleted successfully");
    },
    // e.g. "Conflict: … is the last active admin".
    onError: (error: Error) =>
      toast.error(error.message || "Failed to delete user"),
  });

  const { mutate: toggleDisabled } = useMutation({
    mutationFn: setUserDisabled,
    onSuccess: async (_, { email, disabled }) => {
      await refetch();
      toast.success(`${email} ${disabled ? "disabled" : "enabled"}`);
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to update user"),
  });

  const meta: UserTableMeta = {
    handleModifyUser: (user, e) => {
      e.stopPropagation();
      setUserToModify(user);
    },
    handleUserKeys: (email, e) => {
      e.stopPropagation();
      setKeysOf(email);
    },
    handleResetPassword: (email, e) => {
      e.stopPropagation();
      setPasswordOf(email);
    },
    handleSetDisabled: (user, e) => {
      e.stopPropagation();
      toggleDisabled({ email: user.email, disabled: !user.disabled });
    },
    handleDeleteUser: (email, e) => {
      e.stopPropagation();
      setUserToDelete(email);
    },
  };

  if (!isAdmin) {
    return null;
  }

  return (
    <div className="h-full flex flex-col gap-4">
      <div className="flex w-full max-w-sm items-center space-x-2">
        <Input
          type="text"
          placeholder="Search users..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
        />
      </div>

      <DataTable
        className="w-full"
        columns={columns}
        data={data}
        isLoading={isLoading}
        meta={meta}
        sorting={sorting}
        setSorting={setSorting}
      />

      <div className="flex justify-end">
        <Button onClick={() => setIsCreateOpen(true)}>Add New User</Button>
      </div>

      <CreateUser
        open={isCreateOpen}
        close={() => setIsCreateOpen(false)}
        onSuccess={() => refetch()}
      />

      <Dialog
        open={!!userToDelete}
        onOpenChange={(open) => (!open ? setUserToDelete(undefined) : null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete User</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this user? This action cannot be
              undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="destructive"
              disabled={isDeleting}
              onClick={() => {
                if (userToDelete) {
                  removeUser(userToDelete);
                }
              }}
            >
              Delete
            </Button>
            <Button
              variant="secondary"
              onClick={() => setUserToDelete(undefined)}
            >
              Cancel
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <ModifyUser
        open={!!userToModify}
        close={() => setUserToModify(undefined)}
        onSuccess={() => {
          refetch();
          setUserToModify(undefined);
        }}
        user={userToModify}
      />

      <UserApiKeys email={keysOf} close={() => setKeysOf(undefined)} />

      <ResetPassword
        email={passwordOf}
        close={() => setPasswordOf(undefined)}
      />
    </div>
  );
}
