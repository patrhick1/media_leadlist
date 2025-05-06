import { type RouteConfig, index, route } from "@react-router/dev/routes";

export default [
  index("routes/_index.tsx"),
  route("home", "routes/home.tsx"),
  route("discovery", "routes/discovery.tsx"),
  route("login", "routes/login.tsx"),
] satisfies RouteConfig;
