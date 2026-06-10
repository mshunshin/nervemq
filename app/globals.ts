// In the bundled build the UI is served by the Rust server on the same origin,
// so the endpoint defaults to "" (relative). For `next dev` against a separately
// running backend, set NEXT_PUBLIC_SERVER_ENDPOINT=http://localhost:8080.
export const SERVER_ENDPOINT = process.env.NEXT_PUBLIC_SERVER_ENDPOINT ?? "";

// Base path for the management API. The SQS-compatible API lives at
// `${SERVER_ENDPOINT}/api/sqs` and is not used by the UI.
export const ADMIN_API = `${SERVER_ENDPOINT}/api/admin`;
