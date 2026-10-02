import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

/** The user's choice; `system` follows the operating system's setting. */
export type Theme = "light" | "dark" | "system";

/**
 * Where the choice is saved. next-themes used the same key, so choices made
 * before the move from Next.js carry over; index.html also reads it, to set
 * the theme before the first paint.
 */
const STORAGE_KEY = "theme";

const ThemeContext = createContext<
  { theme: Theme; setTheme: (theme: Theme) => void } | undefined
>(undefined);

function savedTheme(): Theme {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    return saved === "light" || saved === "dark" ? saved : "system";
  } catch {
    return "system";
  }
}

/** Sets `data-theme` on <html>, which globals.css keys the palette off. */
function apply(theme: Theme) {
  const dark =
    theme === "dark" ||
    (theme === "system" &&
      matchMedia("(prefers-color-scheme: dark)").matches);
  const root = document.documentElement;
  root.dataset.theme = dark ? "dark" : "light";
  root.style.colorScheme = dark ? "dark" : "light";
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(savedTheme);

  useEffect(() => {
    apply(theme);
    if (theme !== "system") {
      return;
    }
    // Follow the operating system when it switches.
    const media = matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => apply("system");
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, [theme]);

  const setTheme = useCallback((next: Theme) => {
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // Storage blocked: the choice lasts until the page is reloaded.
    }
    setThemeState(next);
  }, []);

  const value = useMemo(() => ({ theme, setTheme }), [theme, setTheme]);

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (context === undefined) {
    throw new Error("useTheme must be used inside <ThemeProvider>");
  }
  return context;
}
