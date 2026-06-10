import { z } from "zod";

export const sendMessageSchema = z.object({
  body: z.string().min(1, "Message body is required"),
  attributes: z.map(z.string(), z.string()),
});

export type SendMessageForm = z.infer<typeof sendMessageSchema>;
