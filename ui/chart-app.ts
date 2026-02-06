import { App } from "@modelcontextprotocol/ext-apps";
import React from "react";
import ReactDOM from "react-dom/client";
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

const e = React.createElement;

interface ChartConfig {
  title: string;
  description?: string;
  chartType: "area" | "bar" | "line" | "pie";
  data: Record<string, unknown>[];
  dataKey: string;
  xAxisKey: string;
  stacked?: boolean;
  multiSeries?: string[];
}

const CHART_PALETTE = [
  "#e8885c",
  "#5ba3cf",
  "#6ec47e",
  "#c4a55a",
  "#b07cc3",
  "#cf6b8b",
  "#55b5a6",
  "#d4845f",
];

function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return e(
    "div",
    { className: "custom-tooltip" },
    e("div", { className: "tooltip-label" }, String(label)),
    ...payload.map((p: any, i: number) =>
      e(
        "div",
        { key: i, className: "tooltip-value", style: { color: p.color } },
        `${p.name}: ${typeof p.value === "number" ? p.value.toLocaleString() : p.value}`
      )
    )
  );
}

function Chart({ config }: { config: ChartConfig }) {
  const {
    chartType,
    data,
    dataKey,
    xAxisKey,
    stacked = false,
    multiSeries,
  } = config;

  const seriesKeys = multiSeries || [dataKey];

  const commonProps = {
    data,
    margin: { top: 4, right: 12, left: -8, bottom: 0 },
  };

  const axisProps = {
    tickLine: false,
    axisLine: false,
    tickMargin: 8,
    tick: { fontSize: 11 },
  };

  if (chartType === "pie") {
    return e(
      ResponsiveContainer,
      { width: "100%", height: "100%" },
      e(
        PieChart,
        {},
        e(
          Pie,
          {
            data,
            cx: "50%",
            cy: "50%",
            innerRadius: 55,
            outerRadius: 110,
            paddingAngle: 2,
            dataKey: seriesKeys[0],
            nameKey: xAxisKey,
            strokeWidth: 0,
            label: ({
              name,
              percent,
            }: {
              name: string;
              percent: number;
            }) => name + " (" + (percent * 100).toFixed(0) + "%)",
          },
          data.map((_, i) =>
            e(Cell, { key: i, fill: CHART_PALETTE[i % CHART_PALETTE.length] })
          )
        ),
        e(Tooltip, { content: e(CustomTooltip as any) }),
        e(Legend, { iconSize: 8 })
      )
    );
  }

  const ChartComponent =
    chartType === "area"
      ? AreaChart
      : chartType === "bar"
        ? BarChart
        : LineChart;
  const DataComponent =
    chartType === "area" ? Area : chartType === "bar" ? Bar : Line;

  return e(
    ResponsiveContainer,
    { width: "100%", height: "100%" },
    e(
      ChartComponent,
      commonProps,
      e(CartesianGrid, {
        strokeDasharray: "none",
        vertical: false,
        stroke: "var(--grid-stroke)",
      }),
      e(XAxis, {
        ...axisProps,
        dataKey: xAxisKey,
        interval: "preserveStartEnd",
      }),
      e(YAxis, {
        ...axisProps,
        tickFormatter: (v: number) =>
          v >= 1000 ? (v / 1000).toFixed(v >= 10000 ? 0 : 1) + "k" : String(v),
        width: 45,
      }),
      e(Tooltip, { content: e(CustomTooltip as any), cursor: { fill: "var(--muted)" } }),
      seriesKeys.length > 1 ? e(Legend, { iconSize: 8 }) : null,
      ...seriesKeys.map((key, i) => {
        const color = CHART_PALETTE[i % CHART_PALETTE.length];
        const props: Record<string, unknown> = {
          key,
          type: "monotone",
          dataKey: key,
          name: key.replace(/_/g, " "),
          stroke: color,
          strokeWidth: 2,
          stackId: stacked ? "stack" : undefined,
          dot: false,
          activeDot: chartType === "line" ? { r: 4, strokeWidth: 0 } : undefined,
        };
        if (chartType === "area") {
          props.fill = color;
          props.fillOpacity = 0.15;
        }
        if (chartType === "bar") {
          props.fill = color;
          props.radius = [3, 3, 0, 0];
          props.maxBarSize = 32;
        }
        return e(DataComponent as any, props);
      })
    )
  );
}

function ChartApp({ config }: { config: ChartConfig }) {
  return e(
    "div",
    { className: "chart-wrapper" },
    e(
      "div",
      { className: "chart-header" },
      e("div", { className: "chart-title" }, config.title),
      config.description
        ? e("div", { className: "chart-description" }, config.description)
        : null
    ),
    e("div", { className: "chart-container" }, e(Chart, { config }))
  );
}

// Initialize MCP App
const app = new App({ name: "DB Explorer Chart", version: "1.0.0" });

const root = ReactDOM.createRoot(document.getElementById("root")!);

function renderChart(config: ChartConfig) {
  root.render(e(ChartApp, { config }));
}

function renderError(message: string) {
  root.render(
    e(
      "div",
      { className: "chart-wrapper" },
      e(
        "div",
        { className: "chart-header" },
        e("div", { className: "chart-title" }, "Error"),
        e("div", { className: "chart-description" }, message)
      )
    )
  );
}

// Handle initial tool result from host
app.ontoolresult = (result: any) => {
  try {
    const textContent = result.content?.find(
      (c: any) => c.type === "text"
    );
    if (textContent) {
      const config: ChartConfig = JSON.parse(textContent.text);
      renderChart(config);
    } else {
      renderError("No chart data received");
    }
  } catch (err) {
    renderError("Failed to parse chart data: " + String(err));
  }
};

app.onerror = (err: any) => {
  console.error("MCP App error:", err);
};

// Connect to host
app.connect();
