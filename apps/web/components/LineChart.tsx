// apps/web/components/LineChart.tsx
"use client";

type Series = { dates: string[]; values: number[]; label: string };

type Props = {
  // Support both call styles:
  series?: Series;
  left?: Series;
  right?: Series;
  overlay?: Series;
  height?: number;
};

export default function LineChart(props: Props) {
  const height = props.height ?? 320;

  // Normalize input
  const series =
    props.series ??
    (() => {
      const { left, right, overlay } = props;
      // combine into multi-series; render simple list if needed
      return null;
    })();

  // Minimal fail-safe rendering to avoid build errors if no data:
  const rows: { label: string; len: number }[] = [];
  if (props.series) rows.push({ label: props.series.label, len: props.series.values?.length ?? 0 });
  if (props.left) rows.push({ label: props.left.label, len: props.left.values?.length ?? 0 });
  if (props.right) rows.push({ label: props.right.label, len: props.right.values?.length ?? 0 });
  if (props.overlay) rows.push({ label: props.overlay.label, len: props.overlay.values?.length ?? 0 });

  return (
    <div style={{ height }} className="w-full flex flex-col gap-1 text-sm text-neutral-700">
      {rows.length === 0 ? (
        <div className="text-neutral-500">No series.</div>
      ) : (
        rows.map((r) => (
          <div key={r.label} className="flex justify-between border-b py-1">
            <span>{r.label}</span>
            <span>{r.len} pts</span>
          </div>
        ))
      )}
      {/* TODO: replace with your real SVG/canvas chart (kept minimal here to fix types and builds). */}
    </div>
  );
}
