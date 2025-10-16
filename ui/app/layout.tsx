import { Geist, Geist_Mono } from "next/font/google";

import Navigation from "@/components/layout/Navigation";

import version from '../version.json';

import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head />
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <div className="container-fluid d-flex flex-column pl-4 pr-4">
          <Navigation />
          {children}
        </div>
        <footer className="p-3" style={{ fontSize: '0.9em', height: '50px' }}>
          Zavod UI vsn {version.git} ALPHA (built {version.buildTime})
        </footer>
      </body>
    </html>
  );
}
