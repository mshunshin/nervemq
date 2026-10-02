import { AuthVerifier } from "@/components/auth-verifier";
import Header from "@/components/header";
import DashboardSidebar from "@/components/sidebar";
import { Outlet } from "react-router";

export default function DashboardLayout() {
  return (
    <>
      <AuthVerifier />
      <DashboardSidebar />

      <div className="flex flex-col w-full min-h-svh bg-background gap-2 px-4">
        <Header className="h-12" />
        <div>
          <Outlet />
        </div>
      </div>
    </>
  );
}
