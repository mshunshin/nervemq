// eslint-config-next 16 ships native flat configs, so no FlatCompat shim.
import coreWebVitals from "eslint-config-next/core-web-vitals";
import typescript from "eslint-config-next/typescript";

const eslintConfig = [
  ...coreWebVitals,
  ...typescript,
  {
    ignores: ["out/**", ".next/**", "target/**"],
  },
];

export default eslintConfig;
