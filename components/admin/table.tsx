"use client";
import type { ColumnDef } from "@tanstack/react-table";
import {
  Trash2,
  Mail,
  Shield,
  Pencil,
  ArrowUpDown,
  Activity,
  KeyRound,
  LockKeyhole,
  Ban,
  CircleCheck,
} from "lucide-react";
import { Button } from "../ui/button";
import type { UserStatistics } from "@/lib/types";

export const columns: ColumnDef<UserStatistics>[] = [
  // {
  //   accessorKey: "name",
  //   header: () => (
  //     <div className="flex items-center gap-2">
  //       <User className="h-4 w-4" />
  //       <span>Name</span>
  //     </div>
  //   ),
  // },
  {
    accessorKey: "email",
    header: ({ column }) => (
      <div className="flex items-center gap-2">
        <Mail className="h-4 w-4" />
        <Button
          variant="ghost"
          className="p-0 hover:bg-transparent"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          <span>Email</span>
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      </div>
    ),
    enableSorting: true,
  },
  {
    accessorKey: "role",
    header: ({ column }) => (
      <div className="flex items-center gap-2">
        <Shield className="h-4 w-4" />
        <Button
          variant="ghost"
          className="p-0 hover:bg-transparent"
          onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
        >
          <span>Role</span>
          <ArrowUpDown className="ml-2 h-4 w-4" />
        </Button>
      </div>
    ),
    enableSorting: true,
  },
  {
    id: "status",
    accessorFn: (user) => (user.disabled ? "disabled" : "active"),
    header: () => (
      <div className="flex items-center gap-2">
        <Activity className="h-4 w-4" />
        <span>Status</span>
      </div>
    ),
    cell: ({ row }) =>
      row.original.disabled ? (
        <span className="text-destructive">Disabled</span>
      ) : (
        <span className="text-muted-foreground">Active</span>
      ),
  },
  // {
  //   accessorKey: "createdAt",
  //   header: () => (
  //     <div className="flex items-center gap-2">
  //       <Calendar className="h-4 w-4" />
  //       <span>Joined</span>
  //     </div>
  //   ),
  //   cell: ({ row }) => new Date(row.original.createdAt).toLocaleDateString(),
  // },
  // {
  //   accessorKey: "lastLogin",
  //   header: () => (
  //     <div className="flex items-center gap-2">
  //       <Clock className="h-4 w-4" />
  //       <span>Last Login</span>
  //     </div>
  //   ),
  //   cell: ({ row }) =>
  //     row.original.lastLogin
  //       ? new Date(row.original.lastLogin).toLocaleDateString()
  //       : "Never",
  // },
  {
    id: "actions",
    cell: (row) => {
      const meta = row.table.options.meta as UserTableMeta | undefined;
      const user = row.row.original;
      return (
        <div className="flex items-center justify-end gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="hover:bg-secondary/80"
            title="Edit role and namespaces"
            onClick={(e) => meta?.handleModifyUser(user, e)}
          >
            <Pencil className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="hover:bg-secondary/80"
            title="API keys"
            onClick={(e) => meta?.handleUserKeys(user.email, e)}
          >
            <KeyRound className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="hover:bg-secondary/80"
            title="Reset password"
            onClick={(e) => meta?.handleResetPassword(user.email, e)}
          >
            <LockKeyhole className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="hover:bg-secondary/80"
            title={user.disabled ? "Enable user" : "Disable user"}
            onClick={(e) => meta?.handleSetDisabled(user, e)}
          >
            {user.disabled ? (
              <CircleCheck className="h-4 w-4" />
            ) : (
              <Ban className="h-4 w-4" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="text-destructive hover:text-destructive hover:bg-destructive/10"
            title="Delete user"
            onClick={(e) => meta?.handleDeleteUser(user.email, e)}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      );
    },
  },
];

/** Row actions the admin page supplies through the table's `meta`. */
export type UserTableMeta = {
  handleModifyUser: (user: UserStatistics, e: React.MouseEvent) => void;
  handleUserKeys: (email: string, e: React.MouseEvent) => void;
  handleResetPassword: (email: string, e: React.MouseEvent) => void;
  handleSetDisabled: (user: UserStatistics, e: React.MouseEvent) => void;
  handleDeleteUser: (email: string, e: React.MouseEvent) => void;
};
