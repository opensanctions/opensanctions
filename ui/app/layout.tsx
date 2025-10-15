import { Geist, Geist_Mono } from "next/font/google";

import Navigation from "@/components/layout/Navigation";

import version from '../version.json';

import "./globals.css";

// TODO: CSS should come last but eslint is complaining
import type { Metadata } from "next";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Zavod Reviews",
  description: "Review, correct and accept automated data extraction results.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <Navigation />
        {children}
        <footer className="p-3" style={{ fontSize: '0.9em', height: '50px' }}>
          Zavod UI vsn {version.git} ALPHA (built {version.buildTime})
        </footer>
      </body>
    </html>
  );
}
