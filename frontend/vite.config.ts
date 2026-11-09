import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

export default defineConfig({
  base: '/ui/assets/',
  build: {
    modulePreload: { polyfill: false },
    outDir: process.env.FACTORY_ASSET_OUT_DIR || fileURLToPath(new URL('../internal/web/assets', import.meta.url)),
    emptyOutDir: true,
    sourcemap: false,
    manifest: false,
    rollupOptions: {
      output: {
        minifyInternalExports: false,
        compact: true,
        entryFileNames: 'dashboard.js',
        chunkFileNames: 'chunk-[hash].js',
        assetFileNames: (asset) => asset.name?.endsWith('.css') ? 'dashboard.css' : '[name][extname]',
      },
    },
  },
});
