document.addEventListener("DOMContentLoaded", function () {
  const nodeSelect = document.getElementById("nodeSelect");
  const logTypeSelect = document.getElementById("logTypeSelect");
  const controls = document.getElementById("controls");
  const histogramDiv = document.getElementById("histogram");
  const tablesDiv = document.getElementById("tables");
  const intervalSelect = document.getElementById("intervalSelect");
  const startTimePicker = document.getElementById("startTimePicker");
  const endTimePicker = document.getElementById("endTimePicker");
  const applyHistogramFilter = document.getElementById("applyHistogramFilter");
  const toggleScaleBtn = document.getElementById("toggleScaleBtn");
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
    const interval = intervalSelect ? parseInt(intervalSelect.value) : 1;
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
    controls.style.display = "";
    nodeSelect.onchange = logTypeSelect.onchange = renderHistogram;
    renderHistogram();
    renderTables();
    renderWarningsTab();
  }

  if (toggleScaleBtn) {
    toggleScaleBtn.onclick = function () {
      histogramScale = histogramScale === "normal" ? "log" : "normal";
      toggleScaleBtn.classList.toggle("active", histogramScale === "log");
      toggleScaleBtn.textContent =
        histogramScale === "log" ? "Normal Scale" : "Log Scale";
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
    // Clear loading spinner before rendering chart
    histogramDiv.innerHTML = "";
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
    // Find all buckets (time intervals)
    let allBuckets = new Set();
    Object.values(messageBuckets).forEach((bucketsObj) => {
      Object.keys(bucketsObj).forEach((b) => allBuckets.add(b));
    });
    allBuckets = Array.from(allBuckets).sort();
    // For each bucket, collect all message counts for hover
    let customdata = allBuckets.map((bucket) => {
      let msgList = [];
      Object.entries(messageBuckets).forEach(([msg, bucketsObj]) => {
        const count = bucketsObj[bucket] || 0;
        if (count > 0) {
          msgList.push({ msg, count, color: msgColorMap[msg] });
        }
      });
      if (msgList.length === 0)
        return "<span style='color:#888;'>No messages</span>";
      return msgList
        .map(
          (m) =>
            `<span style='color:${m.color};font-size:1.2em;'>&#9679;</span> ${m.count} - ${m.msg}`
        )
        .join("<br>");
    });
    // Prepare traces: one per message
    let traces = Object.entries(messageBuckets).map(([msg, bucketsObj], i) => {
      const color = msgColorMap[msg];
      return {
        x: allBuckets,
        y: allBuckets.map((b) => bucketsObj[b] || 0),
        name: msg,
        type: "bar",
        marker: {
          color: color,
          line: { width: 0 },
          opacity: 0.92,
        },
        // Only first trace gets the custom hover block
        hovertemplate: i === 0 ? "%{x}<br>%{customdata}<extra></extra>" : null,
        customdata: i === 0 ? customdata : undefined,
        hoverinfo: i === 0 ? undefined : "skip",
        showlegend: true,
      };
    });
    Plotly.newPlot(
      histogramDiv,
      traces,
      {
        barmode: "group",
        title: {
          font: {
            family: "Inter, Arial, sans-serif",
            size: 22,
            color: "#172447",
          },
          x: 0.02,
        },
        plot_bgcolor: "#F5F6FA",
        paper_bgcolor: "#F5F6FA",
        font: { family: "Inter, Arial, sans-serif", color: "#172447" },
        xaxis: {
          title: { font: { size: 16, color: "#4a5568" } }, // hide x-axis title
          tickangle: -45,
          gridcolor: "#e2e8f0",
          linecolor: "#e2e8f0",
          tickfont: { size: 13 },
        },
        yaxis: {
          title: { font: { size: 16, color: "#4a5568" } },
          gridcolor: "#e2e8f0",
          zeroline: false,
          tickfont: { size: 13 },
          type: histogramScale === "log" ? "log" : "linear",
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
        hoverlabel: {
          bgcolor: "#F5F6FA",
          bordercolor: "black", // visible border color
          font: { color: "#172447", size: 15 },
        },
        hovermode: "x", // Enable hover on closed bar (entire x-axis)
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
    let nodeIdx = 0;
    Object.entries(jsonData.nodes).forEach(([node, nodeData]) => {
      let nodeHtml = "";
      Object.entries(nodeData).forEach(([logType, logTypeData]) => {
        const logMessages = logTypeData.logMessages || {};
        if (Object.keys(logMessages).length === 0) return;
        nodeHtml += `<h4> ${logType}</h4>`;
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
            <th>UUID</th>
            <th>Support Bundle Name</th>
            <th>Cluster Name</th>
            <th>Organization</th>
            <th>Cluster UUID</th>
            <th>Case ID</th>
            <th>Created At</th>
            <th>View</th>
          </tr></thead><tbody>`;
          sameCluster.forEach((r) => {
            html += `<tr>
              <td>${r.id}</td>
              <td>${r.support_bundle_name}</td>
              <td>${r.cluster_name || ""}</td>
              <td>${r.organization || ""}</td>
              <td>${r.cluster_uuid || ""}</td>
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
            <th>UUID</th>
            <th>Support Bundle Name</th>
            <th>Cluster Name</th>
            <th>Organization</th>
            <th>Cluster UUID</th>
            <th>Case ID</th>
            <th>Created At</th>
            <th>View</th>
          </tr></thead><tbody>`;
          sameOrg.forEach((r) => {
            html += `<tr>
              <td>${r.id}</td>
              <td>${r.support_bundle_name}</td>
              <td>${r.cluster_name || ""}</td
              <td>${r.organization || ""}</td>
              <td>${r.cluster_uuid || ""}</td>
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
