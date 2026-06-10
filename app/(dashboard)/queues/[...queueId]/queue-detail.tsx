"use client";
import MessageList from "@/app/(dashboard)/queues/list";
import { useQuery } from "@tanstack/react-query";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import type { QueueStatistics } from "@/components/queues/table";
import { fetchQueue } from "@/lib/actions/api";
import { QueueSettings } from "@/components/queue-settings";
import { Spinner } from "@heroui/react";
import AccessDenied from "@/components/access-denied";
import NotFound from "@/components/not-found";
import { useParams } from "next/navigation";
import { useEffect, useState } from "react";

// The static export prerenders a single placeholder shell (/queues/_/_), and on
// a hard load useParams() returns the baked-in "_" segments rather than the
// live URL. In that case resolve the real namespace/name from
// window.location; client-side navigations provide real params directly.
function useQueueId(): [string?, string?] {
  const { queueId } = useParams<{ queueId: [string, string] }>();
  const [fromUrl, setFromUrl] = useState<[string, string] | undefined>();

  const isPlaceholder = !queueId || queueId[0] === "_";

  useEffect(() => {
    if (!isPlaceholder) return;
    const segments = window.location.pathname.split("/").filter(Boolean);
    const idx = segments.indexOf("queues");
    const namespace = segments[idx + 1];
    const name = segments[idx + 2];
    if (namespace && name) {
      // One extra render on hard loads only, by design: the URL is a
      // browser-only value that can't be known during prerender/hydration.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setFromUrl([decodeURIComponent(namespace), decodeURIComponent(name)]);
    }
  }, [isPlaceholder]);

  return isPlaceholder ? (fromUrl ?? []) : queueId;
}

function Metric({
  title,
  value,
  isLoading = false,
}: {
  title: string;
  value: React.ReactNode;
  isLoading: boolean;
}) {
  return (
    <div>
      <p className="text-gray-600 wrap-break-word">{title}</p>
      {isLoading ? (
        <div className="relative flex items-center justify-start">
          <Spinner size="sm" className="absolute" />
          <p className="text-2xl font-medium opacity-0">{"0"}</p>
        </div>
      ) : (
        <p className="text-2xl font-medium">{value}</p>
      )}
    </div>
  );
}

export default function QueueDetail() {
  const [namespace, name] = useQueueId();

  const {
    data: queue,
    error,
    isLoading,
  } = useQuery<QueueStatistics, Error>({
    queryKey: ["queues", name, namespace],
    queryFn: () => {
      if (!name || !namespace) {
        throw new Error("Invalid queue ID");
      }
      return fetchQueue(namespace, name) as Promise<QueueStatistics>;
    },
    enabled: !!namespace && !!name,
    refetchInterval: 30000,
  });

  // Until the placeholder shell has resolved the real queue id from the URL,
  // there's nothing to fetch or display yet.
  if (!namespace || !name) {
    return (
      <div className="flex items-center justify-center p-8">
        <Spinner size="lg" />
      </div>
    );
  }

  if (
    error !== null &&
    // FIXME: Improve error handling here
    error.message === "Access Denied"
  ) {
    return <AccessDenied returnTo={{ name: "Queues", href: "/queues" }} />;
  }

  if (queue === undefined && !isLoading) {
    return (
      <NotFound
        resource="queue"
        returnTo={{ name: "Queues", href: "/queues" }}
      />
    );
  }

  return (
    <>
      <div className="grid gap-4">
        {/* Queue Status Section */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Status</CardTitle>
            <QueueSettings queue={queue} />
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <Metric
                title="Pending"
                value={queue?.pending ?? "0"}
                isLoading={isLoading}
              />
              <Metric
                title="Delivered"
                value={queue?.delivered ?? "0"}
                isLoading={isLoading}
              />
              <Metric
                title="Failed"
                value={queue?.failed ?? "0"}
                isLoading={isLoading}
              />
            </div>
          </CardContent>
        </Card>

        {/* Metrics Section */}
        <Card>
          <CardHeader>
            <CardTitle>Metrics</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <Metric
                title="Message Size (avg)"
                value={`${(queue?.avg_size_bytes ?? 0).toFixed(2)} bytes`}
                isLoading={isLoading}
              />
              <Metric
                title="Error Rate"
                value={`${((queue?.failed ?? 0) + (queue?.delivered ?? 0) === 0 ? 0 : ((queue?.failed ?? 0) / ((queue?.delivered ?? 0) + (queue?.failed ?? 0))) * 100).toFixed(2)}%`}
                isLoading={isLoading}
              />
            </div>
          </CardContent>
        </Card>

        {/* Current Queue Items */}
        <Card>
          <CardHeader>
            <CardTitle>Messages</CardTitle>
          </CardHeader>
          <CardContent>
            <MessageList queue={name} namespace={namespace} />
          </CardContent>
        </Card>
      </div>
    </>
  );
}
