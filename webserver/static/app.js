document.addEventListener("DOMContentLoaded", function () {
  const uploadForm = document.getElementById("uploadForm");
  const jsonFile = document.getElementById("jsonFile");
  const nodeSelect = document.getElementById("nodeSelect");
  const logTypeSelect = document.getElementById("logTypeSelect");
  const controls = document.getElementById("controls");
  const histogramDiv = document.getElementById("histogram");
  const tablesDiv = document.getElementById("tables");
  let jsonData = null;

  uploadForm.onsubmit = function (e) {
    e.preventDefault();
    const file = jsonFile.files[0];
    if (!file) return;
    const formData = new FormData();
    formData.append("file", file);
    fetch("/upload", { method: "POST", body: formData })
      .then((res) => res.json())
      .then(() => fetch("/data"))
      .then((res) => res.json())
      .then((data) => {
        jsonData = data;
        renderControlsAndData();
      });
  };

  function renderControlsAndData() {
    // Populate node and log type selectors
    const nodes = Object.keys(jsonData.nodes);
    let logTypes = new Set();
    nodes.forEach((node) => {
      Object.keys(jsonData.nodes[node]).forEach((type) => logTypes.add(type));
    });
    nodeSelect.innerHTML =
      '<option value="all">All</option>' +
      nodes.map((n) => `<option value="${n}">${n}</option>`).join("");
    logTypeSelect.innerHTML =
      '<option value="all">All</option>' +
      Array.from(logTypes)
        .map((t) => `<option value="${t}">${t}</option>`)
        .join("");
    controls.style.display = "";
    nodeSelect.onchange = logTypeSelect.onchange = renderHistogram;
    renderHistogram();
    renderTables();
  }

  function renderHistogram() {
    if (!jsonData) return;
    let selectedNode = nodeSelect.value;
    let selectedType = logTypeSelect.value;
    let messageBuckets = {};
    // Collect histogram per log message
    Object.entries(jsonData.nodes).forEach(([node, nodeData]) => {
      if (selectedNode !== "all" && node !== selectedNode) return;
      Object.entries(nodeData).forEach(([logType, logTypeData]) => {
        if (selectedType !== "all" && logType !== selectedType) return;
        Object.entries(logTypeData.logMessages || {}).forEach(
          ([msg, msgStats]) => {
            Object.entries(msgStats.histogram || {}).forEach(
              ([bucket, count]) => {
                if (!messageBuckets[msg]) messageBuckets[msg] = {};
                messageBuckets[msg][bucket] =
                  (messageBuckets[msg][bucket] || 0) + count;
              }
            );
          }
        );
      });
    });
    // Find all buckets (time intervals)
    let allBuckets = new Set();
    Object.values(messageBuckets).forEach((bucketsObj) => {
      Object.keys(bucketsObj).forEach((b) => allBuckets.add(b));
    });
    allBuckets = Array.from(allBuckets).sort();
    // Use a diverse color palette for better distinction
    const diverseColors = [
      "#172447",
      "#ff9400",
      "#36a2eb",
      "#e74c3c",
      "#2ecc71",
      "#9b59b6",
      "#f1c40f",
      "#16a085",
      "#e67e22",
      "#34495e",
      "#8e44ad",
      "#27ae60",
      "#d35400",
      "#c0392b",
      "#2980b9",
      "#f39c12",
      "#7f8c8d",
      "#1abc9c",
      "#b71540",
      "#60a3bc",
    ];
    // Prepare traces: one per log message
    let traces = Object.entries(messageBuckets).map(([msg, bucketsObj], i) => ({
      x: allBuckets,
      y: allBuckets.map((b) => bucketsObj[b] || 0),
      name: msg,
      type: "bar",
      marker: {
        color: diverseColors[i % diverseColors.length],
        line: { width: 0 },
        opacity: 0.92,
      },
      hovertemplate:
        "<b>%{y} Occurrences</b><br>Time: %{x}<br>Log: " +
        msg +
        "<extra></extra>",
    }));
    Plotly.newPlot(
      histogramDiv,
      traces,
      {
        barmode: "group",
        title: {
          text: "Histogram of Log Messages",
          font: {
            family: "Inter, Arial, sans-serif",
            size: 22,
            color: "#172447",
          },
          x: 0.02,
        },
        plot_bgcolor: "#f5f6fa",
        paper_bgcolor: "#f5f6fa",
        font: { family: "Inter, Arial, sans-serif", color: "#172447" },
        xaxis: {
          title: { text: "Time", font: { size: 16, color: "#4a5568" } },
          tickangle: -45,
          gridcolor: "#e2e8f0",
          linecolor: "#e2e8f0",
          tickfont: { size: 13 },
        },
        yaxis: {
          title: { text: "Occurrences", font: { size: 16, color: "#4a5568" } },
          gridcolor: "#e2e8f0",
          zeroline: false,
          tickfont: { size: 13 },
        },
        margin: { b: 120, t: 60, l: 60, r: 30 },
        legend: {
          orientation: "h",
          y: -0.25,
          font: { size: 15, family: "Inter, Arial, sans-serif" },
          bgcolor: "rgba(255,255,255,0.0)",
        },
        width: histogramDiv.offsetWidth,
        height: 600,
        bargap: 0.18,
        bargroupgap: 0.08,
      },
      { responsive: true, displayModeBar: false }
    );
  }

  window.addEventListener("resize", function () {
    if (jsonData) renderHistogram();
  });

  function renderTables() {
    if (!jsonData) return;
    let html = "";
    Object.entries(jsonData.nodes).forEach(([node, nodeData]) => {
      let nodeHtml = "";
      Object.entries(nodeData).forEach(([logType, logTypeData]) => {
        const logMessages = logTypeData.logMessages || {};
        if (Object.keys(logMessages).length === 0) return; // Skip empty log types
        nodeHtml += `<h4>Process: ${logType}</h4>`;
        nodeHtml +=
          "<table><tr><th>Log Message</th><th>First Occurrence</th><th>Last Occurrence</th><th>Count</th></tr>";
        Object.entries(logMessages).forEach(([msg, stats]) => {
          nodeHtml += `<tr><td>${msg}</td><td>${
            stats.StartTime || ""
          }</td><td>${stats.EndTime || ""}</td><td>${
            stats.count || 0
          }</td></tr>`;
        });
        nodeHtml += "</table>";
      });
      if (nodeHtml) {
        html += `<div class="node-table"><h3>Node: ${node}</h3>${nodeHtml}</div>`;
      }
    });
    tablesDiv.innerHTML = html;
  }
});
