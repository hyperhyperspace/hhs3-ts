import { defineConfig } from "vite";

export default defineConfig({
    base: './',
    build: {
        target: 'es2021',
        sourcemap: true,
    },
});
