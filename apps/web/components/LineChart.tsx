'use client';
import { Line, Bar } from 'react-chartjs-2';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend } from 'chart.js';
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend);

export default function LineChart({ dates, price, sentiment, overlay=true }:{dates:string[],price:number[],sentiment:number[],overlay?:boolean}){
  const dataLine = { labels: dates, datasets: [{ label:'Stock Price', data: price, yAxisID:'y' }] };
  const dataOverlay = { labels: dates, datasets: [{ type:'line', label:'Stock Price', data: price, yAxisID:'y' },
                                                  { type:'bar', label:'Sentiment Score', data: sentiment, yAxisID:'y1' }] };
  const options:any = { responsive:true, interaction:{mode:'index',intersect:false},
    scales:{ y:{ type:'linear', position:'left'}, y1:{ type:'linear', position:'right', grid:{ drawOnChartArea:false } } } };
  return overlay ? <Line data={dataOverlay as any} options={options} /> : (
    <div className="space-y-6">
      <Line data={dataLine as any} options={options} />
      <Bar data={{labels:dates, datasets:[{label:'Sentiment Score', data: sentiment, yAxisID:'y1'}]}} options={options} />
    </div>
  );
}
