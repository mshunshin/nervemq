import QueueDetail from "./queue-detail";

// Static export requires enumerable params for dynamic routes. Queue ids are
// only known at runtime, so we emit a single placeholder shell; the Rust server
// serves it for any /queues/<ns>/<name> deep link. On a hard load useParams()
// yields the baked-in "_" placeholders, so the client component falls back to
// reading the real segments from window.location (see useQueueId).
export function generateStaticParams() {
  return [{ queueId: ["_", "_"] }];
}

export default function QueuePage() {
  return <QueueDetail />;
}
