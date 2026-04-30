import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#101410",
        moss: "#243126",
        cream: "#f3ead2",
        brass: "#c68e3f",
        ember: "#d9573f",
        glade: "#708f63"
      },
      fontFamily: {
        display: ["Constantia", "Cambria", "Georgia", "serif"],
        body: ["Bahnschrift", "Aptos", "Segoe UI", "sans-serif"]
      },
      boxShadow: {
        panel: "0 24px 70px rgba(16, 20, 16, 0.20)"
      }
    }
  },
  plugins: []
};

export default config;
