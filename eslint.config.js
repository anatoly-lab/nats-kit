// Flat ESLint config (ESLint 9+).
//
// Enforces the "framework-free core" discipline:
//   - packages/nats-kit-core/src   — no @nestjs/* imports (framework-free core)
//   - packages/nats-kit-nestjs/src — @nestjs/* allowed (it IS the adapter)

import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import importPlugin from "eslint-plugin-import";

/** Framework packages forbidden in the framework-free NATS core. */
const CORE_FORBIDDEN_PACKAGES = [
  "@nestjs/common",
  "@nestjs/core",
  "@nestjs/platform-express",
  "reflect-metadata",
];

const restrictedImportRule = (patterns, message) => [
  "error",
  {
    paths: patterns.map((name) => ({ name, message })),
    patterns: patterns.map((name) => ({
      group: [`${name}/*`],
      message,
    })),
  },
];

export default tseslint.config(
  {
    // Global ignores.
    ignores: [
      "**/dist/**",
      "**/node_modules/**",
      "**/.turbo/**",
      "**/coverage/**",
      "**/*.tsbuildinfo",
    ],
  },

  js.configs.recommended,
  ...tseslint.configs.recommended,

  {
    // Baseline for every TS source file in the repo.
    files: ["packages/*/src/**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: { ...globals.node },
    },
    plugins: {
      import: importPlugin,
    },
    settings: {
      "import/resolver": {
        typescript: {
          project: ["packages/*/tsconfig.json"],
        },
        node: true,
      },
    },
    rules: {
      // SOLID / typing discipline.
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/consistent-type-imports": [
        "error",
        { prefer: "type-imports", fixStyle: "inline-type-imports" },
      ],
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],

      // Catches phantom imports — keeps each package's declared deps honest.
      "import/no-extraneous-dependencies": [
        "error",
        {
          devDependencies: [
            "**/*.test.ts",
            "**/*.test.tsx",
            "**/*.spec.ts",
            "**/__tests__/**",
            "**/vitest.config.*",
            "**/eslint.config.*",
          ],
        },
      ],

      // Forbid console.* in library code.
      "no-console": "error",
    },
  },

  // ---- NATS core: framework-free ----
  {
    files: ["packages/nats-kit-core/src/**/*.{ts,tsx}"],
    rules: {
      "no-restricted-imports": restrictedImportRule(
        CORE_FORBIDDEN_PACKAGES,
        "The NATS core must remain framework-free; framework code lives in adapters (nats-kit-nestjs, etc.).",
      ),
    },
  },

  // ---- Test files: relax a couple of rules ----
  {
    files: ["**/__tests__/**/*.{ts,tsx}", "**/*.test.{ts,tsx}"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "no-console": "off",
    },
  },

  // ---- Config files at the root: Node-context, allow devDependencies. ----
  {
    files: ["*.{js,mjs,cjs,ts}", "**/vitest.config.{ts,mts}"],
    languageOptions: {
      globals: { ...globals.node },
    },
    rules: {
      "import/no-extraneous-dependencies": "off",
    },
  },
);
