import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Canadian Housing Dashboard",
  description:
    "Interactive dashboard analyzing Canadian rental housing affordability across 18 CMAs using CMHC data.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
