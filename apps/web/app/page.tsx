import HomeClient from "./HomeClient";

export const dynamic = "force-static"; // prerender shell; client fetch gets data at runtime

export default function Page() {
  return <HomeClient />;
}
