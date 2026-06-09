import type { ValidationError } from "yup";

type FieldErrorMap = Record<string, { message: string }[]>;

interface SyncYupSchema {
  validateSync(value: unknown, options: { abortEarly: boolean }): unknown;
}

/**
 * Adapts a yup schema into a synchronous TanStack Form validator.
 *
 * yup's Standard Schema implementation (`schema["~standard"].validate`) resolves
 * asynchronously, so it can't be passed to Form's sync validator slots
 * (`onChange`/`onMount`/`onSubmit`) — and there is no `onMountAsync` slot to move
 * it to. yup's `validateSync` lets us validate synchronously and map each issue
 * onto its field, producing the same `{ message }[]` shape that native Standard
 * Schema validators (e.g. zod) yield for `field.state.meta.errors`.
 */
export function yupSync(schema: SyncYupSchema) {
  return ({ value }: { value: unknown }) => {
    try {
      schema.validateSync(value, { abortEarly: false });
      return undefined;
    } catch (err) {
      const validationError = err as ValidationError;
      const fields: FieldErrorMap = {};
      for (const issue of validationError.inner ?? []) {
        if (!issue.path) continue;
        (fields[issue.path] ??= []).push({ message: issue.message });
      }
      // A schema-level error with no field path becomes a form-wide error.
      if (Object.keys(fields).length === 0) {
        return { form: validationError.message };
      }
      return { fields };
    }
  };
}
