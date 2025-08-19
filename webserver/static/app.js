document.addEventListener("DOMContentLoaded", function () {
  const nodeSelect = document.getElementById("nodeSelect");
  const logTypeSelect = document.getElementById("logTypeSelect");
  const histogramDiv = document.getElementById("histogram");
  const tablesDiv = document.getElementById("tables");
  const intervalSelect = document.getElementById("intervalSelect");
  const startTimePicker = document.getElementById("startTimePicker");
  const endTimePicker = document.getElementById("endTimePicker");
  const applyHistogramFilter = document.getElementById("applyHistogramFilter");
  const toggleScaleBtnChart = document.getElementById("toggleScaleBtnChart");
  const quickRangeSelect = document.getElementById("quickRangeSelect");
  let jsonData = null;
  let currentReportId = null;
  let histogramScale = "normal"; // 'normal' or 'log'

  // Auto-load report if report_uuid is present in the template context
  const reportUuidFromTemplate = window.report_uuid || null;
  if (reportUuidFromTemplate) {
    currentReportId = reportUuidFromTemplate;
    // Instead of fetching /api/reports, fetch histogram with default interval and no range
    fetchAndRenderHistogram();
  }

  // Helper to get ISO string for API from datetime-local input
  function toApiIso(dtStr) {
    if (!dtStr) return null;
    // dtStr is 'YYYY-MM-DDTHH:MM', convert to 'YYYY-MM-DDTHH:MM:00Z'
    return dtStr + ":00Z";
  }

  // Helper to get query params from URL
  function getQueryParams() {
    const params = new URLSearchParams(window.location.search);
    return {
      interval: params.get("interval"),
      start: params.get("start"),
      end: params.get("end"),
    };
  }

  // Helper to update URL with current filter state
  function updateUrlWithFilters() {
    const params = new URLSearchParams(window.location.search);
    if (intervalSelect && intervalSelect.value)
      params.set("interval", intervalSelect.value);
    if (startTimePicker && startTimePicker.value)
      params.set("start", toApiIso(startTimePicker.value));
    else params.delete("start");
    if (endTimePicker && endTimePicker.value)
      params.set("end", toApiIso(endTimePicker.value));
    else params.delete("end");
    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, "", newUrl);
  }

  // On page load, set controls from URL if present
  document.addEventListener("DOMContentLoaded", function () {
    const params = getQueryParams();
    if (intervalSelect && params.interval)
      intervalSelect.value = params.interval;
    if (startTimePicker && params.start)
      startTimePicker.value = params.start.slice(0, 16);
    if (endTimePicker && params.end)
      endTimePicker.value = params.end.slice(0, 16);
  });

  // Update fetchAndRenderHistogram to update URL
  function fetchAndRenderHistogram() {
    updateUrlWithFilters();
    if (!currentReportId) return;
    // Show loading animation in histogram tab
    if (histogramDiv) {
      histogramDiv.innerHTML =
        '<div style="display:flex;align-items:center;justify-content:center;height:400px;"><span class="loader" style="width:48px;height:48px;border:6px solid #e2e8f0;border-top:6px solid #36a2eb;border-radius:50%;animation:spin 1s linear infinite;margin-right:16px;"></span> <span style="font-size:1.2em;color:#888;">Loading histogram...</span></div>';
    }
    const interval = intervalSelect ? parseInt(intervalSelect.value) : 60;
    const start = toApiIso(startTimePicker ? startTimePicker.value : null);
    const end = toApiIso(endTimePicker ? endTimePicker.value : null);
    let url = `/api/histogram/${currentReportId}?interval=${interval}`;
    if (start) url += `&start=${encodeURIComponent(start)}`;
    if (end) url += `&end=${encodeURIComponent(end)}`;
    fetch(url)
      .then((res) => res.json())
      .then((data) => {
        jsonData = data;
        renderControlsAndData();
      });
  }

  if (applyHistogramFilter) {
    applyHistogramFilter.onclick = fetchAndRenderHistogram;
  }
  if (intervalSelect) {
    intervalSelect.onchange = fetchAndRenderHistogram;
  }

  // Optionally, auto-load histogram on page load
  if (intervalSelect && startTimePicker && endTimePicker) {
    fetchAndRenderHistogram();
  }

  // Helper to fetch the latest datetime from the backend
  async function fetchLatestDatetime() {
    if (!currentReportId) return null;
    const resp = await fetch(
      `/api/histogram_latest_datetime/${currentReportId}`
    );
    const data = await resp.json();
    return data.latest_datetime ? new Date(data.latest_datetime) : null;
  }

  // Helper to set start/end pickers based on quick range
  async function handleQuickRangeChange() {
    const val = quickRangeSelect.value;
    if (!val) return;
    if (val === "all") {
      startTimePicker.value = "";
      endTimePicker.value = "";
      fetchAndRenderHistogram();
      return;
    }
    const days = parseInt(val);
    const latest = await fetchLatestDatetime();
    if (!latest) {
      startTimePicker.value = "";
      endTimePicker.value = "";
      fetchAndRenderHistogram();
      return;
    }
    // Set end to latest, start to latest - days
    const end = new Date(latest);
    const start = new Date(latest);
    start.setDate(start.getDate() - days + 1); // inclusive
    // Format as yyyy-MM-ddTHH:mm for datetime-local
    function toLocal(dt) {
      const pad = (n) => n.toString().padStart(2, "0");
      return (
        dt.getFullYear() +
        "-" +
        pad(dt.getMonth() + 1) +
        "-" +
        pad(dt.getDate()) +
        "T" +
        pad(dt.getHours()) +
        ":" +
        pad(dt.getMinutes())
      );
    }
    startTimePicker.value = toLocal(start);
    endTimePicker.value = toLocal(end);
    fetchAndRenderHistogram();
  }

  if (quickRangeSelect) {
    quickRangeSelect.onchange = handleQuickRangeChange;
  }

  // On initial load, set quick range to Last 7 days and trigger change
  document.addEventListener("DOMContentLoaded", function () {
    if (quickRangeSelect) {
      quickRangeSelect.value = "7";
      handleQuickRangeChange();
    }
  });

  function renderWarningsTab() {
    const warningsTabBtn = document.querySelector(
      '.tab-btn[data-tab="warnings-tab"]'
    );
    const warningsPanel = document.getElementById("warnings-tab");
    const warningsDiv = document.getElementById("warnings");
    if (!jsonData || !jsonData.warnings || jsonData.warnings.length === 0) {
      warningsTabBtn.style.display = "none";
      if (warningsPanel) warningsPanel.style.display = "none";
      return;
    }
    warningsTabBtn.style.display = "";
    let html = '<div class="warnings-list">';
    jsonData.warnings.forEach((warn, idx) => {
      html += `<div class="log-solution-collapsible" style="margin-bottom: 12px; border: 1px solid #e2e8f0; border-radius: 6px; background: #fafbfc;">
        <div class="log-solution-header" data-idx="${idx}" style="cursor:pointer; display:flex; align-items:center; padding: 12px 18px; font-weight:600; font-size:1.08em; color:#172447; border-radius:6px 6px 0 0; background:#f1f3f7; transition:background 0.2s;">
          <span class="arrow" style="margin-right:10px; font-size:1.2em; color:#888;">&#9654;</span>
          <span>${warn.message}</span>
        </div>
        <div class="log-solution-body" style="display:none; padding: 18px; background: #fff; border-radius:0 0 6px 6px; border-top:1px solid #e2e8f0; color:#172447;">
          <div><b>Message:</b> ${warn.message}</div>
          ${warn.node ? `<div><b>Node:</b> ${warn.node}</div>` : ""}
          ${
            warn.file
              ? `<div><b>File:</b> <span style='word-break:break-all;'>${warn.file}</span></div>`
              : ""
          }
          ${warn.type ? `<div><b>Type:</b> ${warn.type}</div>` : ""}
          ${warn.level ? `<div><b>Level:</b> ${warn.level}</div>` : ""}
          ${
            warn.additional_details
              ? `<div><b>Details:</b> ${warn.additional_details}</div>`
              : ""
          }
        </div>
      </div>`;
    });
    html += "</div>";
    warningsDiv.innerHTML = html;
    // Accordion logic: only one open at a time
    const headers = warningsDiv.querySelectorAll(".log-solution-header");
    headers.forEach((header) => {
      header.onclick = function () {
        const content = header.nextElementSibling;
        const arrow = header.querySelector(".arrow");
        const isOpen = content.style.display === "block";
        if (isOpen) {
          content.style.display = "none";
          arrow.innerHTML = "&#9654;";
        } else {
          // Collapse all
          warningsDiv.querySelectorAll(".log-solution-body").forEach((c) => {
            c.style.display = "none";
          });
          warningsDiv
            .querySelectorAll(".log-solution-header .arrow")
            .forEach((a) => {
              a.innerHTML = "&#9654;";
            });
          // Expand this one
          content.style.display = "block";
          arrow.innerHTML = "&#9660;";
        }
      };
    });
  }

  // Call renderWarningsTab after jsonData is loaded
  function renderControlsAndData() {
    // Populate node and log type selectors
    const nodes = Object.keys(jsonData.nodes);
    let logTypes = new Set();
    nodes.forEach((node) => {
      Object.keys(jsonData.nodes[node])
        .filter((type) => type !== "node_info")
        .forEach((type) => logTypes.add(type));
    });
    nodeSelect.innerHTML =
      '<option value="all">All</option>' +
      nodes.map((n) => `<option value="${n}">${n}</option>`).join("");
    logTypeSelect.innerHTML =
      '<option value="all">All</option>' +
      Array.from(logTypes)
        .map((t) => `<option value="${t}">${t}</option>`)
        .join("");
    nodeSelect.onchange = logTypeSelect.onchange = renderHistogram;
    renderHistogram();
    renderTables();
    renderWarningsTab();
  }

  if (toggleScaleBtnChart) {
    toggleScaleBtnChart.onclick = function () {
      histogramScale = histogramScale === "normal" ? "log" : "normal";
      toggleScaleBtnChart.classList.toggle("active", histogramScale === "log");
      const label = toggleScaleBtnChart.querySelector(".toggle-label");
      if (label) {
        label.textContent =
          histogramScale === "log" ? "Normal Scale" : "Log Scale";
      }
      renderHistogram();
    };
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
    // Find min and max bucket times
    let allBucketTimes = [];
    Object.values(messageBuckets).forEach((bucketsObj) => {
      Object.keys(bucketsObj).forEach((b) => allBucketTimes.push(b));
    });
    if (allBucketTimes.length === 0) {
      histogramDiv.innerHTML = "<em>No histogram data available.</em>";
      // Also clear dashboards grid
      const dashboardsGrid = document.getElementById("log-message-dashboards");
      if (dashboardsGrid) dashboardsGrid.innerHTML = "";
      return;
    }
    // Parse ISO strings to Date objects
    let allDates = allBucketTimes.map((b) => new Date(b));
    let minDate = new Date(Math.min(...allDates.map((d) => d.getTime())));
    let maxDate = new Date(Math.max(...allDates.map((d) => d.getTime())));
    // Get interval in minutes from intervalSelect
    const intervalMinutes = intervalSelect ? parseInt(intervalSelect.value) : 1;
    // Generate all buckets between minDate and maxDate at intervalMinutes
    let allBuckets = [];
    let curDate = new Date(minDate);
    while (curDate <= maxDate) {
      allBuckets.push(curDate.toISOString().slice(0, 19) + "Z");
      curDate = new Date(curDate.getTime() + intervalMinutes * 60000);
    }
    // For each message, fill missing buckets with zero
    Object.keys(messageBuckets).forEach((msg) => {
      let bucketsObj = messageBuckets[msg];
      allBuckets.forEach((b) => {
        if (!(b in bucketsObj)) bucketsObj[b] = 0;
      });
    });
    // Clear loading spinner before rendering chart
    histogramDiv.innerHTML = "";
    // Create canvas for Chart.js
    const canvas = document.createElement("canvas");
    canvas.id = "histogramChart";
    canvas.height = 600;
    histogramDiv.appendChild(canvas);
    // Add double-click event to reset zoom
    canvas.ondblclick = function () {
      if (window.histogramChartInstance) {
        window.histogramChartInstance.resetZoom();
      }
    };
    // Prepare color mapping for each message
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
    const msgColorMap = {};
    Object.keys(messageBuckets).forEach((msg, i) => {
      msgColorMap[msg] = diverseColors[i % diverseColors.length];
    });
    // Prepare datasets for Chart.js
    const datasets = Object.entries(messageBuckets).map(
      ([msg, bucketsObj], i) => ({
        label: msg,
        data: allBuckets.map((b) => bucketsObj[b] || 0),
        backgroundColor: msgColorMap[msg],
        borderWidth: 1,
        borderColor: msgColorMap[msg],
        barPercentage: 0.9,
        categoryPercentage: 0.8,
      })
    );
    // Chart.js config for main histogram
    const chartConfig = {
      type: "bar",
      data: {
        labels: allBuckets,
        datasets: datasets,
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "bottom",
            labels: {
              font: { size: 15, family: "Inter, Arial, sans-serif" },
              boxWidth: 18,
              padding: 18,
            },
            align: "start",
          },
          title: { display: false },
          tooltip: {
            mode: "index",
            intersect: false,
            callbacks: {
              label: function (context) {
                if (
                  !context.parsed ||
                  context.parsed.y == null ||
                  context.parsed.y === 0
                )
                  return "";
                return `${context.dataset.label}: ${context.parsed.y}`;
              },
            },
          },
          zoom: {
            pan: { enabled: true, mode: "x" },
            zoom: {
              wheel: { enabled: false },
              pinch: { enabled: true },
              drag: { enabled: true, modifierKey: null },
              mode: "x",
            },
            limits: {
              x: { min: 0, max: allBuckets.length - 1 },
              y: { min: 0 },
            },
          },
        },
        scales: {
          x: {
            title: { display: false },
            ticks: {
              color: "#4a5568",
              font: { size: 13 },
              callback: function (val, idx, ticks) {
                const N = Math.ceil(ticks.length / 8);
                if (idx === 0 || idx === ticks.length - 1 || idx % N === 0) {
                  const dt = this.getLabelForValue(val);
                  if (dt.length > 16 && dt.includes("T")) {
                    const date = dt.slice(5, 10);
                    const time = dt.slice(11, 16);
                    return `${date} ${time}`;
                  }
                  return dt;
                }
                return "";
              },
              maxRotation: 0,
              minRotation: 0,
              autoSkip: false,
            },
            grid: { color: "#e2e8f0", display: false },
          },
          y: {
            title: {
              display: true,
              text: "Count",
              color: "#4a5568",
              font: { size: 16 },
            },
            ticks: { color: "#4a5568", font: { size: 13 } },
            grid: { color: "#e2e8f0", display: false },
            type: histogramScale === "log" ? "logarithmic" : "linear",
            beginAtZero: true,
          },
        },
      },
    };
    // Render Chart.js
    const chartInstance = new Chart(canvas, chartConfig);
    window.histogramChartInstance = chartInstance;

    // --- Grafana-like dashboards for each log message ---
    const dashboardsGrid = document.getElementById("log-message-dashboards");
    if (!dashboardsGrid) return;
    dashboardsGrid.innerHTML = "";
    // For each log message, collect per-node histogram data
    const nodeList = Object.keys(jsonData.nodes);
    const nodeColors = {};
    nodeList.forEach((n, i) => {
      nodeColors[n] = diverseColors[i % diverseColors.length];
    });
    Object.keys(messageBuckets).forEach((msg, msgIdx) => {
      // For this message, collect per-node histogram
      let perNodeBuckets = {};
      nodeList.forEach((node) => {
        perNodeBuckets[node] = {};
      });
      // Fill per-node buckets
      Object.entries(jsonData.nodes).forEach(([node, nodeData]) => {
        Object.entries(nodeData).forEach(([logType, logTypeData]) => {
          Object.entries(logTypeData.logMessages || {}).forEach(
            ([m, msgStats]) => {
              if (m !== msg) return;
              Object.entries(msgStats.histogram || {}).forEach(
                ([bucket, count]) => {
                  perNodeBuckets[node][bucket] =
                    (perNodeBuckets[node][bucket] || 0) + count;
                }
              );
            }
          );
        });
      });
      // Fill missing buckets with zero
      nodeList.forEach((node) => {
        allBuckets.forEach((b) => {
          if (!(b in perNodeBuckets[node])) perNodeBuckets[node][b] = 0;
        });
      });
      // Dashboard card
      const card = document.createElement("div");
      card.className = "dashboard-card";
      // Title
      const title = document.createElement("div");
      title.className = "dashboard-title";
      title.textContent = msg;
      card.appendChild(title);
      // Chart canvas
      const chartDiv = document.createElement("div");
      chartDiv.className = "dashboard-canvas";
      const chartCanvas = document.createElement("canvas");
      chartCanvas.height = 234;
      chartCanvas.width = 510;
      chartDiv.appendChild(chartCanvas);
      card.appendChild(chartDiv);
      // Popup button
      const popupBtn = document.createElement("button");
      popupBtn.className = "dashboard-popup-btn";
      popupBtn.title = "Open larger view";
      popupBtn.innerHTML =
        '<svg width="22" height="22" viewBox="0 0 22 22" fill="none"><rect x="3.5" y="3.5" width="15" height="15" rx="3.5" stroke="currentColor" stroke-width="2"/><path d="M7 7H15V15" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
      popupBtn.onclick = function (e) {
        e.stopPropagation();
        openDashboardPopup(msg, perNodeBuckets, allBuckets, nodeColors);
      };
      card.appendChild(popupBtn);
      // --- Dashboard popup modal logic ---
      function openDashboardPopup(msg, perNodeBuckets, allBuckets, nodeColors) {
        // Remove any existing popup
        let oldPopup = document.getElementById("dashboard-popup-overlay");
        if (oldPopup) oldPopup.remove();
        // Create overlay
        const overlay = document.createElement("div");
        overlay.className = "dashboard-popup-overlay";
        overlay.id = "dashboard-popup-overlay";
        // Modal
        const modal = document.createElement("div");
        modal.className = "dashboard-popup-modal";
        // Title
        const title = document.createElement("div");
        title.className = "dashboard-popup-title";
        title.textContent = msg;
        modal.appendChild(title);
        // Chart canvas
        const chartDiv = document.createElement("div");
        chartDiv.className = "dashboard-popup-canvas";
        const chartCanvas = document.createElement("canvas");
        chartCanvas.height = 510;
        chartCanvas.width = 1350;
        chartDiv.appendChild(chartCanvas);
        modal.appendChild(chartDiv);
        // Legend
        const nodeList = Object.keys(perNodeBuckets);
        let activeNodes = new Set(nodeList);
        const legend = document.createElement("div");
        legend.className = "dashboard-popup-legend";
        nodeList.forEach((node) => {
          const label = document.createElement("span");
          label.className = "dashboard-popup-legend-label active";
          // Bullet
          const bullet = document.createElement("span");
          bullet.className = "legend-bullet";
          bullet.style.background = nodeColors[node];
          label.appendChild(bullet);
          // Text
          const text = document.createElement("span");
          text.textContent = node;
          label.appendChild(text);
          label.style.color = nodeColors[node];
          label.onclick = function () {
            if (activeNodes.has(node)) {
              activeNodes.delete(node);
              label.classList.remove("active");
              label.classList.add("inactive");
              label.style.color = "#b0b0b0";
            } else {
              activeNodes.add(node);
              label.classList.add("active");
              label.classList.remove("inactive");
              label.style.color = nodeColors[node];
            }
            renderPopupChart();
          };
          legend.appendChild(label);
        });
        modal.appendChild(legend);
        // Render chart
        let chartInstance = null;
        function renderPopupChart() {
          if (chartInstance) chartInstance.destroy();
          const datasets = nodeList
            .filter((node) => activeNodes.has(node))
            .map((node) => ({
              label: node,
              data: allBuckets.map((b) => perNodeBuckets[node][b] || 0),
              backgroundColor: nodeColors[node],
              borderColor: nodeColors[node],
              borderWidth: 2,
              fill: false,
              pointRadius: 2.5,
              tension: 0.2,
            }));
          chartInstance = new Chart(chartCanvas, {
            type: "line",
            data: {
              labels: allBuckets,
              datasets: datasets,
            },
            options: {
              responsive: false,
              maintainAspectRatio: false,
              plugins: {
                legend: { display: false },
                title: { display: false },
                tooltip: {
                  mode: "index",
                  intersect: false,
                  callbacks: {
                    label: function (context) {
                      if (
                        !context.parsed ||
                        context.parsed.y == null ||
                        context.parsed.y === 0
                      )
                        return "";
                      return `${context.dataset.label}: ${context.parsed.y}`;
                    },
                  },
                },
                zoom: {
                  pan: { enabled: true, mode: "x" },
                  zoom: {
                    wheel: { enabled: false },
                    pinch: { enabled: true },
                    drag: { enabled: true, modifierKey: null },
                    mode: "x",
                  },
                  limits: {
                    x: { min: 0, max: allBuckets.length - 1 },
                    y: { min: 0 },
                  },
                },
              },
              scales: {
                x: {
                  title: { display: false },
                  ticks: {
                    color: "#4a5568",
                    font: { size: 13 },
                    callback: function (val, idx, ticks) {
                      const N = Math.ceil(ticks.length / 10);
                      if (
                        idx === 0 ||
                        idx === ticks.length - 1 ||
                        idx % N === 0
                      ) {
                        const dt = this.getLabelForValue(val);
                        if (dt.length > 16 && dt.includes("T")) {
                          const date = dt.slice(5, 10);
                          const time = dt.slice(11, 16);
                          return `${date} ${time}`;
                        }
                        return dt;
                      }
                      return "";
                    },
                    maxRotation: 0,
                    minRotation: 0,
                    autoSkip: false,
                  },
                  grid: { color: "#e2e8f0", display: false },
                },
                y: {
                  title: {
                    display: true,
                    text: "Count",
                    color: "#4a5568",
                    font: { size: 15 },
                  },
                  ticks: { color: "#4a5568", font: { size: 13 } },
                  grid: { color: "#e2e8f0", display: false },
                  type: histogramScale === "log" ? "logarithmic" : "linear",
                  beginAtZero: true,
                },
              },
            },
          });
          // Double-click to reset zoom
          chartCanvas.ondblclick = function () {
            if (chartInstance && chartInstance.resetZoom)
              chartInstance.resetZoom();
          };
        }
        renderPopupChart();
        // Overlay click closes popup (except modal itself)
        overlay.onclick = function (e) {
          if (e.target === overlay) overlay.remove();
        };
        // Keyboard Esc closes popup
        document.addEventListener("keydown", function escListener(e) {
          if (e.key === "Escape") {
            overlay.remove();
            document.removeEventListener("keydown", escListener);
          }
        });
        // Add to body
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        // Ensure popup is visible in viewport
        setTimeout(() => {
          overlay.scrollIntoView({ behavior: "smooth", block: "center" });
        }, 0);
      }
      // Legend (node labels) - moved below chart
      const legend = document.createElement("div");
      legend.className = "dashboard-legend";
      let activeNodes = new Set(nodeList);
      nodeList.forEach((node) => {
        const label = document.createElement("span");
        label.className = "dashboard-legend-label active";
        // Add colored bullet
        const bullet = document.createElement("span");
        bullet.className = "legend-bullet";
        bullet.style.background = nodeColors[node];
        label.appendChild(bullet);
        // Add node name
        const text = document.createElement("span");
        text.textContent = node;
        label.appendChild(text);
        label.style.color = nodeColors[node];
        label.onclick = function () {
          if (activeNodes.has(node)) {
            activeNodes.delete(node);
            label.classList.remove("active");
            label.classList.add("inactive");
            label.style.color = "#b0b0b0";
          } else {
            activeNodes.add(node);
            label.classList.add("active");
            label.classList.remove("inactive");
            label.style.color = nodeColors[node];
          }
          renderChart();
        };
        legend.appendChild(label);
      });
      card.appendChild(legend);
      dashboardsGrid.appendChild(card);
      // Render chart function
      let chartInstance = null;
      function renderChart() {
        if (chartInstance) chartInstance.destroy();
        const datasets = nodeList
          .filter((node) => activeNodes.has(node))
          .map((node) => ({
            label: node,
            data: allBuckets.map((b) => perNodeBuckets[node][b] || 0),
            backgroundColor: nodeColors[node],
            borderColor: nodeColors[node],
            borderWidth: 2,
            fill: false,
            pointRadius: 1.5,
            tension: 0.2,
          }));
        chartInstance = new Chart(chartCanvas, {
          type: "line",
          data: {
            labels: allBuckets,
            datasets: datasets,
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              title: { display: false },
              tooltip: {
                mode: "index",
                intersect: false,
                callbacks: {
                  label: function (context) {
                    if (
                      !context.parsed ||
                      context.parsed.y == null ||
                      context.parsed.y === 0
                    )
                      return "";
                    return `${context.dataset.label}: ${context.parsed.y}`;
                  },
                },
              },
              zoom: {
                pan: { enabled: true, mode: "x" },
                zoom: {
                  wheel: { enabled: false },
                  pinch: { enabled: true },
                  drag: { enabled: true, modifierKey: null },
                  mode: "x",
                },
                limits: {
                  x: { min: 0, max: allBuckets.length - 1 },
                  y: { min: 0 },
                },
              },
            },
            scales: {
              x: {
                title: { display: false },
                ticks: {
                  color: "#4a5568",
                  font: { size: 11 },
                  callback: function (val, idx, ticks) {
                    const N = Math.ceil(ticks.length / 6);
                    if (
                      idx === 0 ||
                      idx === ticks.length - 1 ||
                      idx % N === 0
                    ) {
                      const dt = this.getLabelForValue(val);
                      if (dt.length > 16 && dt.includes("T")) {
                        const date = dt.slice(5, 10);
                        const time = dt.slice(11, 16);
                        return `${date} ${time}`;
                      }
                      return dt;
                    }
                    return "";
                  },
                  maxRotation: 0,
                  minRotation: 0,
                  autoSkip: false,
                },
                grid: { color: "#e2e8f0", display: false },
              },
              y: {
                title: {
                  display: true,
                  text: "Count",
                  color: "#4a5568",
                  font: { size: 13 },
                },
                ticks: { color: "#4a5568", font: { size: 11 } },
                grid: { color: "#e2e8f0", display: false },
                type: histogramScale === "log" ? "logarithmic" : "linear",
                beginAtZero: true,
              },
            },
          },
        });
        // Double-click to reset zoom
        chartCanvas.ondblclick = function () {
          if (chartInstance && chartInstance.resetZoom)
            chartInstance.resetZoom();
        };
      }
      renderChart();
    });
  }

  window.addEventListener("resize", function () {
    if (jsonData) renderHistogram();
  });

  function renderTables() {
    if (!jsonData) return;
    let html = "";
    let nodeIdx = 0;
    Object.entries(jsonData.nodes).forEach(([node, nodeData]) => {
      let nodeHtml = "";
      Object.entries(nodeData).forEach(([logType, logTypeData]) => {
        const logMessages = logTypeData.logMessages || {};
        if (Object.keys(logMessages).length === 0) return;
        nodeHtml += `<h4> ${logType}</h4>`;
        nodeHtml +=
          "<table class='table-view-log-table'><tr>" +
          "<th class='log-msg-col'>Log Message</th>" +
          "<th class='first-occ-col'>First Occurrence</th>" +
          "<th class='last-occ-col'>Last Occurrence</th>" +
          "<th class='count-col'>Count</th></tr>";
        Object.entries(logMessages).forEach(([msg, stats]) => {
          nodeHtml +=
            `<tr>` +
            `<td class='log-msg-col'>${msg}</td>` +
            `<td class='first-occ-col'>${stats.StartTime || ""}</td>` +
            `<td class='last-occ-col'>${stats.EndTime || ""}</td>` +
            `<td class='count-col'>${stats.count || 0}</td>` +
            `</tr>`;
        });
        nodeHtml += "</table>";
      });
      if (nodeHtml) {
        html += `
          <div class="node-table-collapsible">
            <div class="node-header" data-node-idx="${nodeIdx}">
              <span class="arrow">&#9654;</span>
              <span>${node}</span>
            </div>
            <div class="node-content" style="display:none;">${nodeHtml}</div>
          </div>
        `;
        nodeIdx++;
      }
    });
    tablesDiv.innerHTML = html;
    // Accordion logic: only one open at a time
    const headers = tablesDiv.querySelectorAll(".node-header");
    headers.forEach((header) => {
      header.onclick = function () {
        const content = header.nextElementSibling;
        const arrow = header.querySelector(".arrow");
        const isOpen = content.style.display === "block";
        if (isOpen) {
          // Collapse this one
          content.style.display = "none";
          arrow.innerHTML = "&#9654;";
        } else {
          // Collapse all
          tablesDiv.querySelectorAll(".node-content").forEach((c) => {
            c.style.display = "none";
          });
          tablesDiv.querySelectorAll(".node-header .arrow").forEach((a) => {
            a.innerHTML = "&#9654;";
          });
          // Expand this one
          content.style.display = "block";
          arrow.innerHTML = "&#9660;";
        }
      };
    });
  }

  // Tab switching logic
  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabPanels = document.querySelectorAll(".tab-panel");
  tabBtns.forEach((btn) => {
    btn.addEventListener("click", function () {
      tabBtns.forEach((b) => b.classList.remove("active"));
      tabPanels.forEach((p) => (p.style.display = "none"));
      btn.classList.add("active");
      const tabId = btn.getAttribute("data-tab");
      document.getElementById(tabId).style.display = "block";
      // Re-render histogram on tab switch for correct sizing
      if (tabId === "histogram-tab" && jsonData) renderHistogram();
      if (tabId === "table-tab" && jsonData) renderTables();
      if (tabId === "gflags-tab" && jsonData) renderGFlags();
      if (tabId === "nodeinfo-tab" && jsonData) renderNodeInfo();
      if (tabId === "logsolutions-tab" && jsonData) renderLogSolutions();
      if (tabId === "related-tab") renderRelatedReports();
    });
  });

  function renderGFlags() {
    const gflagsDiv = document.getElementById("gflags");
    gflagsDiv.innerHTML = "<em>Loading GFlags...</em>";
    if (!window.report_uuid) {
      gflagsDiv.innerHTML = "<em>No report selected.</em>";
      return;
    }
    fetch(`/api/gflags/${window.report_uuid}`)
      .then((resp) => resp.json())
      .then((gflagsData) => {
        if (
          !gflagsData ||
          Object.keys(gflagsData).length === 0 ||
          gflagsData.error
        ) {
          gflagsDiv.innerHTML = "<em>No GFlags data available.</em>";
          return;
        }
        let html = "";
        // Master GFlags collapsible
        if (gflagsData.master) {
          let masterHtml = "<table><tr><th>Flag</th><th>Value</th></tr>";
          Object.entries(gflagsData.master).forEach(([k, v]) => {
            masterHtml += `<tr><td>${k}</td><td>${v}</td></tr>`;
          });
          masterHtml += "</table>";
          html += `
            <div class="node-table-collapsible">
              <div class="node-header gflag-header" data-node-idx="gflag-master">
                <span class="arrow">&#9654;</span>
                <span>Master GFlags</span>
              </div>
              <div class="node-content" style="display:none;">${masterHtml}</div>
            </div>
          `;
        }
        // TServer GFlags collapsible
        if (gflagsData.tserver) {
          let tserverHtml = "<table><tr><th>Flag</th><th>Value</th></tr>";
          Object.entries(gflagsData.tserver).forEach(([k, v]) => {
            tserverHtml += `<tr><td>${k}</td><td>${v}</td></tr>`;
          });
          tserverHtml += "</table>";
          html += `
            <div class="node-table-collapsible">
              <div class="node-header gflag-header" data-node-idx="gflag-tserver">
                <span class="arrow">&#9654;</span>
                <span>TServer GFlags</span>
              </div>
              <div class="node-content" style="display:none;">${tserverHtml}</div>
            </div>
          `;
        }
        // Controller GFlags collapsible
        if (gflagsData.controller) {
          let controllerHtml = "<table><tr><th>Flag</th><th>Value</th></tr>";
          Object.entries(gflagsData.controller).forEach(([k, v]) => {
            controllerHtml += `<tr><td>${k}</td><td>${v}</td></tr>`;
          });
          controllerHtml += "</table>";
          html += `
            <div class="node-table-collapsible">
              <div class="node-header gflag-header" data-node-idx="gflag-controller">
                <span class="arrow">&#9654;</span>
                <span>Controller GFlags</span>
              </div>
              <div class="node-content" style="display:none;">${controllerHtml}</div>
            </div>
          `;
        }
        if (!html)
          html = "<em>No GFlags found for Master, TServer, or Controller.</em>";
        gflagsDiv.innerHTML = html;
        // Accordion logic for Master/TServer/Controller
        const gflagHeaders = gflagsDiv.querySelectorAll(".gflag-header");
        gflagHeaders.forEach((header) => {
          header.onclick = function () {
            const content = header.nextElementSibling;
            const arrow = header.querySelector(".arrow");
            const isOpen = content.style.display === "block";
            if (isOpen) {
              content.style.display = "none";
              arrow.innerHTML = "&#9654;";
            } else {
              // Collapse all
              gflagsDiv.querySelectorAll(".node-content").forEach((c) => {
                c.style.display = "none";
              });
              gflagsDiv
                .querySelectorAll(".gflag-header .arrow")
                .forEach((a) => {
                  a.innerHTML = "&#9654;";
                });
              content.style.display = "block";
              arrow.innerHTML = "&#9660;";
            }
          };
        });
      })
      .catch(() => {
        gflagsDiv.innerHTML = "<em>Failed to load GFlags data.</em>";
      });
  }

  function renderNodeInfo() {
    const nodeinfoDiv = document.getElementById("nodeinfo");
    nodeinfoDiv.innerHTML = "<em>Loading node info...</em>";
    if (!window.report_uuid) {
      nodeinfoDiv.innerHTML = "<em>No report selected.</em>";
      return;
    }
    fetch(`/api/node_info/${window.report_uuid}`)
      .then((resp) => resp.json())
      .then((data) => {
        if (!data || data.error) {
          nodeinfoDiv.innerHTML = "<em>No node info available.</em>";
          return;
        }
        // New columns and pretty names
        const columns = [
          { key: "node_name", label: "Node Name" },
          { key: "state", label: "State" },
          { key: "is_master", label: "Is Master" },
          { key: "is_tserver", label: "Is TServer" },
          { key: "placement", label: "Placement" },
          { key: "num_cores", label: "Cores" },
          { key: "mem_size_gb", label: "Memory (GB)" },
          { key: "volume_size_gb", label: "Volume Size (GB)" },
        ];
        let html = `<div class='node-table-collapsible'>
          <div class='node-header nodeinfo-header' data-node-idx='nodeinfo-all'>
            <span class='arrow'>&#9654;</span>
            <span>All Nodes (${data.nodes.length})</span>
          </div>
          <div class='node-content' style='display:none; overflow-x:auto;'>`;
        if (data.nodes.length === 0) {
          html += "<em>No nodes found.</em>";
        } else {
          html += `<table style='min-width:900px;'><tr>`;
          columns.forEach((col) => {
            html += `<th>${col.label}</th>`;
          });
          html += "</tr>";
          data.nodes.forEach((node) => {
            html += "<tr>";
            columns.forEach((col) => {
              html += `<td>${
                node[col.key] !== null && node[col.key] !== undefined
                  ? node[col.key]
                  : ""
              }</td>`;
            });
            html += "</tr>";
          });
          html += "</table>";
        }
        html += "</div></div>";
        nodeinfoDiv.innerHTML = html;
        // Accordion logic for the section
        const nodeinfoHeader = nodeinfoDiv.querySelector(".nodeinfo-header");
        nodeinfoHeader.onclick = function () {
          const content = nodeinfoHeader.nextElementSibling;
          const arrow = nodeinfoHeader.querySelector(".arrow");
          const isOpen = content.style.display === "block";
          if (isOpen) {
            content.style.display = "none";
            arrow.innerHTML = "&#9654;";
          } else {
            content.style.display = "block";
            arrow.innerHTML = "&#9660;";
          }
        };
        // Optionally, expand by default
        const firstBody = nodeinfoDiv.querySelector(".node-content");
        const firstArrow = nodeinfoDiv.querySelector(".nodeinfo-header .arrow");
        if (firstBody && firstArrow) {
          firstBody.style.display = "block";
          firstArrow.innerHTML = "&#9660;";
        }
      })
      .catch(() => {
        nodeinfoDiv.innerHTML = "<em>Failed to load node info.</em>";
      });
  }

  function renderLogSolutions() {
    const logsolutionsDiv = document.getElementById("logsolutions");
    if (!jsonData) {
      logsolutionsDiv.innerHTML = "<em>No log data loaded.</em>";
      return;
    }
    const foundMessages = new Set();
    Object.values(jsonData.nodes).forEach((nodeData) => {
      Object.values(nodeData).forEach((logTypeData) => {
        if (logTypeData.logMessages) {
          Object.keys(logTypeData.logMessages).forEach((msg) =>
            foundMessages.add(msg)
          );
        }
      });
    });
    const solutionsMap = window.logSolutionsMap || {};
    let html = "";
    if (foundMessages.size === 0) {
      html = "<em>No log messages found in this report.</em>";
    } else {
      html = '<div class="log-solutions-list">';
      const converter = new showdown.Converter();
      let idx = 0;
      foundMessages.forEach((msg) => {
        const solutionMd =
          solutionsMap[msg] ||
          "<em>No solution available for this log message.</em>";
        const solutionHtml = converter.makeHtml(solutionMd);
        html += `<div class=\"log-solution-collapsible\" style=\"margin-bottom: 12px; border: 1px solid #e2e8f0; border-radius: 6px; background: #fafbfc;\">
          <div class=\"log-solution-header\" data-idx=\"${idx}\" style=\"cursor:pointer; display:flex; align-items:center; padding: 12px 18px; font-weight:600; font-size:1.08em; color:#172447; border-radius:6px 6px 0 0; background:#f1f3f7; transition:background 0.2s;\">
            <span class=\"arrow\" style=\"margin-right:10px; font-size:1.2em; color:#888;\">&#9654;</span>
            <span>${msg}</span>
          </div>
          <div class=\"log-solution-body\" style=\"display:none; padding: 18px; background: #fff; border-radius:0 0 6px 6px; border-top:1px solid #e2e8f0;\">${solutionHtml}</div>
        </div>`;
        idx++;
      });
      html += "</div>";
    }
    logsolutionsDiv.innerHTML = html;
    // Accordion logic: only one open at a time
    const headers = logsolutionsDiv.querySelectorAll(".log-solution-header");
    headers.forEach((header) => {
      header.onmouseenter = function () {
        header.style.background = "#e6eaf3";
      };
      header.onmouseleave = function () {
        header.style.background = "#f1f3f7";
      };
      header.onclick = function () {
        const content = header.nextElementSibling;
        const arrow = header.querySelector(".arrow");
        const isOpen = content.style.display === "block";
        if (isOpen) {
          content.style.display = "none";
          arrow.innerHTML = "&#9654;";
        } else {
          // Collapse all
          logsolutionsDiv
            .querySelectorAll(".log-solution-body")
            .forEach((c) => {
              c.style.display = "none";
            });
          logsolutionsDiv
            .querySelectorAll(".log-solution-header .arrow")
            .forEach((a) => {
              a.innerHTML = "&#9654;";
            });
          // Expand this one
          content.style.display = "block";
          arrow.innerHTML = "&#9660;";
        }
      };
    });
  }

  function renderRelatedReports() {
    const relatedDiv = document.getElementById("related-reports");
    relatedDiv.innerHTML = "<em>Loading related reports...</em>";
    if (!window.report_uuid) {
      relatedDiv.innerHTML = "<em>No report selected.</em>";
      return;
    }
    fetch(`/api/related_reports/${window.report_uuid}`)
      .then((resp) => resp.json())
      .then((related) => {
        if (!related || related.error) {
          relatedDiv.innerHTML = "<em>No related reports found.</em>";
          return;
        }
        const sameCluster = related.same_cluster || [];
        const sameOrg = related.same_org || [];
        let html = "";
        // Expand/collapse for cluster
        html += `<div class='related-section-collapsible'>
          <div class='related-section-header' style='cursor:pointer; display:flex; align-items:center; font-weight:600; font-size:1.15em; background:#f5f6fa; border-radius:8px 8px 0 0; padding:12px 18px; margin-bottom:0;'>
            <span class='arrow' style='margin-right:10px; font-size:1.2em; color:#888;'>&#9654;</span>
            Reports for Cluster: <span style='color:#172447; margin-left:8px;'>${
              sameCluster.length > 0
                ? sameCluster[0].cluster_name || sameCluster[0].cluster_uuid
                : "(none)"
            }</span>
          </div>
          <div class='related-section-body' style='display:none; padding:0 0 18px 0; background:#fff; border-radius:0 0 8px 8px;'>`;
        if (sameCluster.length === 0) {
          html +=
            "<em style='margin-left:18px;'>No other reports for this cluster.</em>";
        } else {
          html += `<table style='margin:18px 0 0 0;'><thead><tr>
            <th>Support Bundle Name</th>
            <th>Organization</th>
            <th>Case ID</th>
            <th>Created At</th>
            <th>View</th>
          </tr></thead><tbody>`;
          sameCluster.forEach((r) => {
            html += `<tr>
              <td>${r.support_bundle_name}</td>
              <td>${r.organization || ""}</td>
              <td>${
                r.case_id
                  ? `<a href='https://yugabyte.zendesk.com/agent/tickets/${r.case_id}' target='_blank'>${r.case_id}</a>`
                  : "-"
              }</td>
              <td>${r.created_at}</td>
              <td><a href="/reports/${r.id}">View Report</a></td>
            </tr>`;
          });
          html += "</tbody></table>";
        }
        html += `</div></div>`;
        // Expand/collapse for org
        html += `<div class='related-section-collapsible' style='margin-top:2em;'>
          <div class='related-section-header' style='cursor:pointer; display:flex; align-items:center; font-weight:600; font-size:1.15em; background:#f5f6fa; border-radius:8px 8px 0 0; padding:12px 18px; margin-bottom:0;'>
            <span class='arrow' style='margin-right:10px; font-size:1.2em; color:#888;'>&#9654;</span>
            Reports for Organization: <span style='color:#172447; margin-left:8px;'>${
              sameOrg.length > 0 ? sameOrg[0].organization || "" : "(none)"
            }</span>
          </div>
          <div class='related-section-body' style='display:none; padding:0 0 18px 0; background:#fff; border-radius:0 0 8px 8px;'>`;
        if (sameOrg.length === 0) {
          html +=
            "<em style='margin-left:18px;'>No other reports for this organization.</em>";
        } else {
          html += `<table style='margin:18px 0 0 0;'><thead><tr>
            <th>Support Bundle Name</th>
            <th>Cluster Name</th>
            <th>Case ID</th>
            <th>Created At</th>
            <th>View</th>
          </tr></thead><tbody>`;
          sameOrg.forEach((r) => {
            html += `<tr>
              <td>${r.support_bundle_name}</td>
              <td>${r.cluster_name || ""}</td
              <td>${
                r.case_id
                  ? `<a href='https://yugabyte.zendesk.com/agent/tickets/${r.case_id}' target='_blank'>${r.case_id}</a>`
                  : "-"
              }</td>
              <td>${r.created_at}</td>
              <td><a href="/reports/${r.id}">View Report</a></td>
            </tr>`;
          });
          html += "</tbody></table>";
        }
        html += `</div></div>`;
        relatedDiv.innerHTML = html;
        // Accordion logic for both sections
        const sectionHeaders = relatedDiv.querySelectorAll(
          ".related-section-header"
        );
        sectionHeaders.forEach((header) => {
          header.onclick = function () {
            const body = header.nextElementSibling;
            const arrow = header.querySelector(".arrow");
            const isOpen = body.style.display === "block";
            if (isOpen) {
              body.style.display = "none";
              arrow.innerHTML = "&#9654;";
            } else {
              // Collapse all
              relatedDiv
                .querySelectorAll(".related-section-body")
                .forEach((b) => {
                  b.style.display = "none";
                });
              relatedDiv
                .querySelectorAll(".related-section-header .arrow")
                .forEach((a) => {
                  a.innerHTML = "&#9654;";
                });
              // Expand this one
              body.style.display = "block";
              arrow.innerHTML = "&#9660;";
            }
          };
        });
        // Optionally, expand the first section by default
        const firstBody = relatedDiv.querySelector(".related-section-body");
        const firstArrow = relatedDiv.querySelector(
          ".related-section-header .arrow"
        );
        if (firstBody && firstArrow) {
          firstBody.style.display = "block";
          firstArrow.innerHTML = "&#9660;";
        }
      })
      .catch(() => {
        relatedDiv.innerHTML = "<em>Failed to load related reports.</em>";
      });
  }

  // Add CSS for spinner animation
  const style = document.createElement("style");
  style.innerHTML = `@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }`;
  document.head.appendChild(style);
});
