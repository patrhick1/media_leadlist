import { defineConfig, loadEnv } from 'vite';
import { reactRouter } from "@react-router/dev/vite";
import tailwindcss from "@tailwindcss/vite";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig(({ mode }) => {
  // Load .env files from the root of the workspace (where Replit secrets are exposed for backend)
  // For frontend, Vite usually loads .env files from the frontend project root (review_ui/)
  // or its parent for VITE_ prefixed variables.
  // const env = loadEnv(mode, process.cwd(), ''); // process.cwd() is project root

  return {
    plugins: [
      tailwindcss(),
      reactRouter(),
      tsconfigPaths()
    ],
    // define: { // Keep this if you were using it for a client-side password previously and still need it as a fallback
    //  'import.meta.env.PGL_LOGIN_PASSWORD_FOR_FRONTEND': JSON.stringify(env.PGL_FRONTEND_PASSWORD_FROM_ROOT_ENV)
    // },
    server: { 
      port: 5173, // Explicitly define frontend dev port if not default
      proxy: {
        // Proxy API requests to your FastAPI backend running on port 8000
        '/auth': { // For /auth/validate-login
          target: 'http://localhost:8000',
          changeOrigin: true,
        },
        '/actions': { // For /actions/search/* and /actions/enrich
          target: 'http://localhost:8000',
          changeOrigin: true,
        },
        '/static': { // For CSV downloads or other static files served by backend
            target: 'http://localhost:8000',
            changeOrigin: true,
        },
        // Add other backend API prefixes if you have them (e.g., /leads, /campaigns, /users, /analytics)
        '/leads': {
            target: 'http://localhost:8000',
            changeOrigin: true,
        },
        '/campaigns': {
            target: 'http://localhost:8000',
            changeOrigin: true,
        },
        '/users': {
            target: 'http://localhost:8000',
            changeOrigin: true,
        },
        '/analytics': {
            target: 'http://localhost:8000',
            changeOrigin: true,
        }
      }
    }
  };
});
