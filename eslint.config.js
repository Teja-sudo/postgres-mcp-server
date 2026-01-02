import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import sonarjs from "eslint-plugin-sonarjs";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  sonarjs.configs.recommended,
  {
    files: ["src/**/*.ts"],
    ignores: ["src/__tests__/**", "dist/**", "node_modules/**", "coverage/**"],
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        project: "./tsconfig.json",
      },
    },
    rules: {
      // ============================================
      // TypeScript ESLint Rules
      // ============================================
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/no-explicit-any": "warn",
      "@typescript-eslint/no-empty-function": "warn",
      "prefer-const": "error",
      "no-console": "off",
      "consistent-return": "off",
      "no-duplicate-imports": "error",

      // ============================================
      // SonarJS - Override recommended with stricter settings
      // ============================================
      // Cognitive complexity - enterprise level
      "sonarjs/cognitive-complexity": ["warn", 15],

      // Security rules - stricter for enterprise
      "sonarjs/no-hardcoded-passwords": "error",
      "sonarjs/no-hardcoded-secrets": "error",
      "sonarjs/code-eval": "error",
      "sonarjs/no-weak-cipher": "error",
      "sonarjs/no-weak-keys": "error",

      // Bug detection - errors
      "sonarjs/no-collection-size-mischeck": "error",
      "sonarjs/no-identical-conditions": "error",
      "sonarjs/no-identical-expressions": "error",
      "sonarjs/no-use-of-empty-return-value": "error",
      "sonarjs/no-element-overwrite": "error",
      "sonarjs/no-extra-arguments": "error",
      "sonarjs/no-unthrown-error": "error",
      "sonarjs/no-primitive-wrappers": "error",
      "sonarjs/no-misleading-array-reverse": "error",
      "sonarjs/arguments-order": "error",

      // Disable some noisy rules for database code
      "sonarjs/sql-queries": "off", // We're a SQL tool, SQL is intentional
      "sonarjs/no-commented-code": "warn",
      "sonarjs/todo-tag": "warn",
      "sonarjs/fixme-tag": "warn",
    },
  },
  {
    ignores: [
      "dist/**",
      "node_modules/**",
      "src/__tests__/**",
      "**/*.test.ts",
      "**/*.spec.ts",
    ],
  }
);
