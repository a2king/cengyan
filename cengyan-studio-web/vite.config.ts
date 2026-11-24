import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/api': { target: 'http://127.0.0.1:19670', changeOrigin: true },
    },
  },
  build: {
    outDir: '../cengyan-studio/src/main/resources/studio',
    emptyOutDir: true,
    cssCodeSplit: false,
    assetsDir: 'assets',
    rollupOptions: {
      output: {
        entryFileNames: 'app.js',
        assetFileNames: (asset) => asset.name?.endsWith('.css') ? 'app.css' : 'assets/[name]-[hash][extname]',
        chunkFileNames: 'assets/[name]-[hash].js',
      },
    },
  },
});
