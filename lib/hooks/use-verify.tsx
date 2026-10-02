import { useEffect, useRef } from "react";
import { useNavigate } from "react-router";
import { useGlobalState } from "@/lib/state/global";
import { adminSessionSchema } from "@/lib/types";
import { ADMIN_API } from "@/lib/actions/api";

export function useVerifyUser(intervalMs: number = 300 * 1000) {
  const navigate = useNavigate();
  const intervalRef = useRef<ReturnType<typeof setInterval> | undefined>(
    undefined,
  );

  useEffect(() => {
    const verify = async () => {
      try {
        const response = await fetch(`${ADMIN_API}/auth/verify`, {
          method: "POST",
          credentials: "include",
          mode: "cors",
        });

        if (!response.ok) {
          // null = verified unauthenticated (vs undefined = not yet checked)
          useGlobalState.setState({ session: null });
          navigate("/login");
          return;
        }

        const data = await response.json();
        useGlobalState.setState({ session: adminSessionSchema.parse(data) });
      } catch {
        useGlobalState.setState({ session: null });
        navigate("/login");
      }
    };

    verify(); // Run immediately
    intervalRef.current = setInterval(verify, intervalMs);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [intervalMs, navigate]);
}
