import MessageList from "@/components/queues/message-list";
import { useQuery } from "@tanstack/react-query";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import type { QueueStatistics } from "@/lib/types";
import { fetchQueue } from "@/lib/actions/api";
import { QueueSettings } from "@/components/queue-settings";
import QueueAttributesCard from "@/components/queue-attributes";
import SendMessage from "@/components/send-message";
import PurgeQueue from "@/components/purge-queue";
import PauseQueue from "@/components/pause-queue";
import { useNamespaceAccess } from "@/lib/hooks/use-namespace-access";
import { Spinner } from "@/components/ui/spinner";
import AccessDenied from "@/components/access-denied";
import NotFound from "@/components/not-found";
import { useParams } from "react-router";

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

/**
 * Shown while the queue is paused: consumers get no messages, and the ones
 * still in flight are what they are finishing. Once none are, the consumers
 * can be swapped.
 */
function PausedNotice({
  pausedAt,
  inFlight,
}: {
  pausedAt: number;
  inFlight: number;
}) {
  return (
    <div className="mb-4 rounded-md border border-destructive/50 p-3 text-sm">
      <p className="font-medium text-destructive">
        Paused since {new Date(pausedAt * 1000).toLocaleString()}
      </p>
      <p className="text-muted-foreground">
        Consumers receive no messages until the queue is resumed.{" "}
        {inFlight === 0
          ? "No messages are in flight: consumers can be swapped."
          : `${inFlight} ${inFlight === 1 ? "message is" : "messages are"} still in flight.`}
      </p>
    </div>
  );
}

export default function QueueDetail() {
  const { namespace, queue: name } = useParams<"namespace" | "queue">();
  // Admins and the namespace's owners manage the queue; other members send
  // and receive only, so the management controls are hidden from them.
  const { canManage } = useNamespaceAccess();
  const manage = canManage(namespace);

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
    // Faster while paused, so draining consumers can be watched.
    refetchInterval: (query) =>
      query.state.data?.paused_at != null ? 5000 : 30000,
  });

  // The route (/queues/:namespace/:queue) always supplies both.
  if (!namespace || !name) {
    return null;
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
            {manage ? (
              <div className="flex items-center gap-2">
                <PauseQueue queue={queue} />
                <QueueSettings queue={queue} />
              </div>
            ) : null}
          </CardHeader>
          <CardContent>
            {queue?.paused_at != null ? (
              <PausedNotice
                pausedAt={queue.paused_at}
                inFlight={queue.delivered}
              />
            ) : null}
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

        {/* Queue Attributes Section */}
        <QueueAttributesCard
          namespace={namespace}
          queue={name}
          editable={manage}
        />

        {/* Current Queue Items */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle>Messages</CardTitle>
            <div className="flex items-center gap-2">
              <SendMessage namespace={namespace} queue={name} />
              {manage ? <PurgeQueue namespace={namespace} queue={name} /> : null}
            </div>
          </CardHeader>
          <CardContent>
            <MessageList
              queue={name}
              namespace={namespace}
              manageable={manage}
            />
          </CardContent>
        </Card>
      </div>
    </>
  );
}
