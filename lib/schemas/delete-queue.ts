import { z } from "zod";

// Names of an existing queue: whatever it was created with (queues made over
// the SQS API are not limited to the UI's alphabet), so only non-empty.
export const deleteQueueSchema = z.object({
  name: z.string().min(1),
  namespace: z.string().min(1),
});

export type DeleteQueueRequest = z.infer<typeof deleteQueueSchema>;
