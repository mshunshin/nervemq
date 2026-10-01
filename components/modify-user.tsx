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

import { useForm } from "@tanstack/react-form";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Label } from "./ui/label";
import { cn } from "@/lib/utils";
import {
  listNamespaces,
  listUserAllowedNamespaces,
  setNamespaceOwner,
  updateUserAllowedNamespaces,
  updateUserRole,
} from "@/lib/actions/api";
import { Spinner } from "@/components/ui/spinner";
import { ChevronsUpDown, Plus, Check } from "lucide-react";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "./ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "./ui/popover";
import { toast } from "sonner";
import { useInvalidate } from "@/lib/hooks/use-invalidate";
import CreateNamespace from "./create-namespace";
import { useState } from "react";
import { modifyUserSchema } from "@/lib/schemas/modify-user";
import type { NamespaceStatistics, UserStatistics } from "@/lib/types";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";
import { Role } from "@/lib/state/global";

/**
 * Edits a user's role, the namespaces they can access, and which of those
 * they own. Everything here acts on `user` — the account being edited, not
 * the logged-in admin.
 */
export default function ModifyUser({
  open,
  close,
  onSuccess,
  user,
}: {
  open: boolean;
  close: () => void;
  onSuccess?: (userName: string) => void;
  user?: UserStatistics;
}) {
  const { data: namespaces } = useQuery({
    queryFn: () => listNamespaces(),
    queryKey: ["namespaces"],
  });

  const { data: userNamespaces } = useQuery({
    queryKey: ["users", "user-namespaces", { email: user?.email }],
    queryFn: () => listUserAllowedNamespaces({ email: user?.email }),
    enabled: user !== undefined,
  });

  return (
    <Dialog
      open={open}
      onOpenChange={(open) => {
        if (!open) {
          close();
        }
      }}
    >
      <DialogContent>
        {user && namespaces && userNamespaces ? (
          // Mounted once its data is in, so the form starts from the user's
          // actual grants rather than an empty set it would then save.
          <ModifyUserForm
            key={user.email}
            user={user}
            namespaces={namespaces}
            userNamespaces={userNamespaces}
            close={close}
            onSuccess={onSuccess}
          />
        ) : (
          <>
            <DialogHeader>
              <DialogTitle>Modify User Access</DialogTitle>
              <DialogDescription>Loading…</DialogDescription>
            </DialogHeader>
            <div className="flex justify-center py-4">
              <Spinner size="sm" />
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}

function ModifyUserForm({
  user,
  namespaces,
  userNamespaces,
  close,
  onSuccess,
}: {
  user: UserStatistics;
  namespaces: NamespaceStatistics[];
  userNamespaces: string[];
  close: () => void;
  onSuccess?: (userName: string) => void;
}) {
  const [showCreateNamespace, setShowCreateNamespace] = useState(false);
  const [nsPopoverOpen, setNsPopoverOpen] = useState(false);

  const [initialOwned] = useState(
    () =>
      new Set(
        namespaces
          .filter((ns) => ns.owners.includes(user.email))
          .map((ns) => ns.name),
      ),
  );

  const invalidateUsers = useInvalidate(["users"]);
  const invalidateNamespaces = useInvalidate(["namespaces"]);

  const { mutateAsync: doModify } = useMutation({
    mutationFn: async (data: {
      namespaces: Set<string>;
      owned: Set<string>;
      role: Role;
    }) => {
      // Sequential: ownership changes apply to the namespace set just saved.
      // Replacing the set keeps ownership of namespaces that stay.
      await updateUserAllowedNamespaces({
        email: user.email,
        namespaces: Array.from(data.namespaces),
      });
      for (const namespace of data.owned) {
        if (!initialOwned.has(namespace)) {
          await setNamespaceOwner({ namespace, email: user.email, owner: true });
        }
      }
      for (const namespace of initialOwned) {
        // Removed namespaces lost their grant, ownership included.
        if (!data.owned.has(namespace) && data.namespaces.has(namespace)) {
          await setNamespaceOwner({ namespace, email: user.email, owner: false });
        }
      }
      if (data.role !== user.role) {
        await updateUserRole({ email: user.email, role: data.role });
      }
    },
    onSettled: () => {
      invalidateUsers();
      invalidateNamespaces();
    },
    onError: (error: Error) =>
      toast.error(error.message || "Failed to update user"),
  });

  const form = useForm({
    defaultValues: {
      namespaces: new Set(userNamespaces) as Set<string>,
      owned: new Set(initialOwned) as Set<string>,
      role: user.role,
    },
    validators: {
      onChange: modifyUserSchema,
      onMount: modifyUserSchema,
      onSubmit: modifyUserSchema,
    },
    onSubmit: async ({ value: data, formApi }) => {
      try {
        await doModify(data);
      } catch {
        // Error toast handled by the mutation's onError.
        return;
      }
      onSuccess?.(user.email);
      close();
      formApi.reset();
    },
  });

  const handleNamespaceCreated = async (namespaceName: string) => {
    form.setFieldValue("namespaces", (set) => {
      set.add(namespaceName);
      return set;
    });
    await form.validateField("namespaces", "change");
    setShowCreateNamespace(false);
  };

  return (
    <>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          e.stopPropagation();
          void form.handleSubmit();
        }}
        className="flex flex-col gap-4"
      >
        <DialogHeader>
          <DialogTitle>Modify User Access</DialogTitle>
          <DialogDescription>
            Role and namespace access for {user.email}.
          </DialogDescription>
        </DialogHeader>
        <form.Field name="role">
          {(field) => (
            <div className="flex flex-col gap-2">
              <Label htmlFor={field.name}>Role</Label>
              <Select
                value={field.state.value}
                onValueChange={(value) => field.handleChange(value as Role)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select a role" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="user">User</SelectItem>
                  <SelectItem value="admin">Admin</SelectItem>
                </SelectContent>
              </Select>
              {field.state.meta.errors.length > 0 ? (
                <span className="text-sm text-destructive">
                  {field.state.meta.errors.map((e) => e?.message).join(", ")}
                </span>
              ) : null}
            </div>
          )}
        </form.Field>
        <form.Field name="namespaces">
          {(field) => (
            <div className="flex flex-col gap-2">
              <Label htmlFor={field.name}>Grant Access to Namespaces</Label>
              <Popover open={nsPopoverOpen} onOpenChange={setNsPopoverOpen}>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    className={cn(
                      "w-full justify-between",
                      field.state.value?.size > 0 ? "" : "text-muted-foreground",
                    )}
                  >
                    <span className="truncate">
                      {field.state.value?.size > 0
                        ? Array.from(field.state.value).join(", ")
                        : "Select namespaces to grant access"}
                    </span>
                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-(--radix-popover-trigger-width) p-0">
                  <Command className="bg-background">
                    <CommandInput placeholder="Search namespace..." />
                    <CommandList>
                      <CommandEmpty>
                        <div className="flex flex-col items-center justify-center py-4 gap-2">
                          <p className="text-sm text-muted-foreground">
                            No namespace found.
                          </p>
                        </div>
                      </CommandEmpty>
                      <CommandGroup>
                        {namespaces.map((namespace) => (
                          <CommandItem
                            key={namespace.name}
                            value={namespace.name}
                            className="cursor-pointer"
                            onSelect={(currentValue) => {
                              const current = new Set(field.state.value);
                              if (current.has(currentValue)) {
                                current.delete(currentValue);
                                // No access, so no ownership either.
                                form.setFieldValue("owned", (owned) => {
                                  const next = new Set(owned);
                                  next.delete(currentValue);
                                  return next;
                                });
                              } else {
                                current.add(currentValue);
                              }
                              field.handleChange(current);
                            }}
                          >
                            <div className="flex items-center gap-2">
                              <div className="w-4 h-4 flex items-center justify-center">
                                {field.state.value.has(namespace.name) && (
                                  <Check className="h-4 w-4" />
                                )}
                              </div>
                              {namespace.name}
                            </div>
                          </CommandItem>
                        ))}
                      </CommandGroup>
                      <CommandGroup>
                        <CommandItem
                          onSelect={() => setShowCreateNamespace(true)}
                          className="flex items-center gap-2 cursor-pointer"
                        >
                          <Plus className="h-4 w-4" />
                          Create Namespace
                        </CommandItem>
                      </CommandGroup>
                    </CommandList>
                  </Command>
                </PopoverContent>
              </Popover>
            </div>
          )}
        </form.Field>
        <form.Subscribe selector={(state) => state.values.namespaces}>
          {(selected) =>
            selected.size > 0 ? (
              <form.Field name="owned">
                {(field) => (
                  <div className="flex flex-col gap-2">
                    <Label>Owner of</Label>
                    <p className="text-sm text-muted-foreground">
                      Owners can delete a namespace and manage its queues;
                      other members only send and receive messages.
                    </p>
                    <div className="flex flex-col gap-1">
                      {Array.from(selected)
                        .sort()
                        .map((namespace) => (
                          <label
                            key={namespace}
                            className="flex items-center gap-2 text-sm cursor-pointer"
                          >
                            <input
                              type="checkbox"
                              className="h-4 w-4 accent-primary"
                              checked={field.state.value.has(namespace)}
                              onChange={(e) => {
                                const next = new Set(field.state.value);
                                if (e.target.checked) {
                                  next.add(namespace);
                                } else {
                                  next.delete(namespace);
                                }
                                field.handleChange(next);
                              }}
                            />
                            {namespace}
                          </label>
                        ))}
                    </div>
                  </div>
                )}
              </form.Field>
            ) : null
          }
        </form.Subscribe>

        <DialogFooter>
          <form.Subscribe
            selector={(state) => [state.canSubmit, state.isSubmitting]}
          >
            {([canSubmit, isSubmitting]) => (
              <>
                <Button type="submit" disabled={!canSubmit}>
                  {isSubmitting ? (
                    <>
                      <Spinner className="absolute self-center" size="sm" />
                      <p className="text-transparent">Save Changes</p>
                    </>
                  ) : (
                    "Save Changes"
                  )}
                </Button>

                <DialogClose asChild>
                  <Button variant={"secondary"} disabled={isSubmitting}>
                    Cancel
                  </Button>
                </DialogClose>
              </>
            )}
          </form.Subscribe>
        </DialogFooter>
      </form>

      <CreateNamespace
        open={showCreateNamespace}
        close={() => setShowCreateNamespace(false)}
        onSuccess={handleNamespaceCreated}
      />
    </>
  );
}
