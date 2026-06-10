import QueueDetail from "./queue-detail";

// Static export requires enumerable params for dynamic routes. Queue ids are
// only known at runtime, so we emit a single placeholder shell; the Rust server
// serves it for any /queues/<ns>/<name> deep link and the client component reads
// the real segments from the live URL via useParams().
export function generateStaticParams() {
  return [{ queueId: ["_", "_"] }];
}

export default function QueuePage() {
  return <QueueDetail />;
}
