import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  // This global ignore object must be at the top
  {
    ignores: ["dist/**", "node_modules/**"]
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{js,mjs,cjs,ts,mts,cts}"],
    // ... rest of your config
  }
);