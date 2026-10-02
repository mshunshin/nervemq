import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router/dom";
import "./globals.css";
import Providers from "./providers";
import { router } from "./router";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Providers sidebarOpen={true}>
      <RouterProvider router={router} />
    </Providers>
  </StrictMode>,
);
