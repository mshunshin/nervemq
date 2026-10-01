import { Label } from "./ui/label";

/**
 * Shown instead of the namespace picker when the user is (or is being made)
 * an admin: admins reach every namespace through their role, so granting
 * namespaces would add nothing — and picking some would wrongly suggest it
 * limits them.
 */
export default function AdminNamespaceNote() {
  return (
    <div className="flex flex-col gap-2">
      <Label>Namespaces</Label>
      <p className="text-sm text-muted-foreground">
        Admins can access and manage every namespace, so there is nothing to
        grant.
      </p>
    </div>
  );
}
