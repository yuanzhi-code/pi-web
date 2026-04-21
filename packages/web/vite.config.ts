import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [vue()],
  build: {
    outDir: "../../web-dist",
    emptyOutDir: true,
    target: "esnext",
    reportCompressedSize: false,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("marked") || id.includes("dompurify")) {
            return "markdown";
          }
          if (id.includes("shiki")) {
            return "shiki";
          }
          if (id.includes("lucide-vue-next")) {
            return "icons";
          }
          if (id.includes("vue/dist") || id === "vue") {
            return "vue";
          }
        },
      },
    },
  },
  server: {
    proxy: {
      "/ws": {
        target: "ws://localhost:8080",
        ws: true,
      },
    },
  },
});
