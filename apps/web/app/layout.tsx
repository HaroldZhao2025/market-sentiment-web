import './globals.css';
export const metadata = { title: 'Market Sentiment — S&P 500', description: 'Daily news & earnings sentiment with price overlays' };
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (<html lang="en"><body><div className="container py-8"><h1 className="text-3xl font-bold mb-6">Market Sentiment — S&amp;P 500</h1>{children}</div></body></html>);
}
