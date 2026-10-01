import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { listNamespaces } from "@/lib/actions/api";

/**
 * What the logged-in user may do in each namespace. Admins and a
 * namespace's owners manage its queues (create, delete, purge, configure,
 * message actions); other members only send and receive. The server enforces
 * this; the UI uses it to hide controls that would be refused.
 *
 * Shares the ["namespaces"] query, so it refreshes with the namespace list.
 */
export function useNamespaceAccess() {
  const { data } = useQuery({
    queryKey: ["namespaces"],
    queryFn: () => listNamespaces(),
  });

  return useMemo(() => {
    const manageable = new Set(
      (data ?? []).filter((ns) => ns.can_manage).map((ns) => ns.name),
    );
    return {
      manageable,
      canManage: (namespace?: string) =>
        namespace !== undefined && manageable.has(namespace),
    };
  }, [data]);
}
