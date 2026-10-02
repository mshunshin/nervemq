import { createBrowserRouter, Navigate } from "react-router";
import DashboardLayout from "./layouts/dashboard";
import AdminPanel from "./routes/admin";
import ApiKeys from "./routes/api-keys";
import RouteError from "./routes/error";
import LoginPage from "./routes/login";
import Namespaces from "./routes/namespaces";
import PageNotFound from "./routes/not-found";
import QueueDetail from "./routes/queue-detail";
import Queues from "./routes/queues";

/**
 * Every page of the admin UI. The server answers any non-API, non-asset path
 * with index.html (src/lib.rs, `mod ui`), so deep links such as
 * /queues/<ns>/<name> load straight into the matching route.
 */
export const router = createBrowserRouter([
  {
    // Catches render errors on every page; without it one white-screens the
    // whole app.
    errorElement: <RouteError />,
    children: [
      { path: "/", element: <Navigate to="/queues" replace /> },
      { path: "/login", element: <LoginPage /> },
      {
        // Sidebar, header and the session check.
        element: <DashboardLayout />,
        children: [
          { path: "/queues", element: <Queues /> },
          { path: "/queues/:namespace/:queue", element: <QueueDetail /> },
          { path: "/namespaces", element: <Namespaces /> },
          { path: "/api-keys", element: <ApiKeys /> },
          { path: "/admin", element: <AdminPanel /> },
        ],
      },
      { path: "*", element: <PageNotFound /> },
    ],
  },
]);
