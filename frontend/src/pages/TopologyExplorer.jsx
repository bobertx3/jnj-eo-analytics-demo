import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Network, ZoomIn, ZoomOut, Maximize2, Info } from 'lucide-react';
import { useApi, formatNumber, formatCurrency } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';
import { DomainBadge } from '../components/SeverityBadge';

const DOMAIN_COLORS = {
  infrastructure: '#bc8cff',
  application: '#58a6ff',
  network: '#39d353',
  unknown: '#8b949e',
};

// Static topology layout positions (manually positioned for clarity)
const NODE_POSITIONS = {
  // Network layer (top)
  'load-balancer':  { x: 400, y: 40 },
  'dns-resolver':   { x: 600, y: 40 },
  'vpn-gateway':    { x: 200, y: 40 },
  // Application layer (middle)
  'patient-portal':           { x: 150, y: 170 },
  'ehr-api':                  { x: 350, y: 170 },
  'fhir-api':                 { x: 550, y: 170 },
  'auth-service':             { x: 750, y: 170 },
  'clinical-decision-support':{ x: 150, y: 300 },
  'pharmacy-service':         { x: 350, y: 300 },
  'imaging-service':          { x: 550, y: 300 },
  'notification-service':     { x: 750, y: 300 },
  'hl7-gateway':              { x: 150, y: 430 },
  'ml-inference-service':     { x: 350, y: 430 },
  'terminology-service':      { x: 550, y: 430 },
  'dicom-gateway':            { x: 750, y: 430 },
  // Infrastructure layer (bottom)
  'ehr-database':       { x: 200, y: 560 },
  'auth-database':      { x: 400, y: 560 },
  'drug-interaction-db':{ x: 600, y: 560 },
  'message-queue':      { x: 350, y: 650 },
  'pacs-storage':       { x: 550, y: 650 },
};

function getNodeColor(node) {
  const risk = Number(node.risk_score) || 0;
  if (risk > 500) return '#f85149';
  if (risk > 200) return '#f0883e';
  if (risk > 50) return '#d29922';
  return DOMAIN_COLORS[node.domain] || '#8b949e';
}

function getNodeSize(node) {
  const risk = Number(node.risk_score) || 0;
  if (risk > 500) return 32;
  if (risk > 200) return 28;
  if (risk > 50) return 24;
  return 20;
}

export default function TopologyExplorer() {
  const { data: topology, loading } = useApi('/api/services/topology');
  const canvasRef = useRef(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [zoom, setZoom] = useState(1);
  const [hoveredNode, setHoveredNode] = useState(null);

  const nodes = topology?.nodes || [];
  const edges = topology?.edges || [];

  // Build node lookup. Use static positions where available, and fall back to
  // a dynamic per-domain layout so new service names still render.
  const nodeMap = {};
  const domainBuckets = {
    network: [],
    application: [],
    infrastructure: [],
    unknown: [],
  };

  nodes.forEach((n) => {
    const domain = (n.domain || 'unknown').toLowerCase();
    if (domainBuckets[domain]) {
      domainBuckets[domain].push(n);
    } else {
      domainBuckets.unknown.push(n);
    }
  });

  const minX = 70;
  const maxX = 850;
  const maxPerRow = 10;
  const rowStep = 72;
  const canvasLogicalHeight = 720;
  const unknownRows = Math.max(1, Math.ceil(domainBuckets.unknown.length / maxPerRow));
  const unknownMaxY = canvasLogicalHeight - 44; // Keep room for node labels near the bottom edge.
  const unknownBaseY = domainBuckets.unknown.length > 0
    ? Math.max(180, unknownMaxY - ((unknownRows - 1) * rowStep))
    : 660;

  const domainBaseY = {
    network: 50,
    application: 180,
    infrastructure: 520,
    unknown: unknownBaseY,
  };

  Object.entries(domainBuckets).forEach(([domain, bucket]) => {
    bucket
      .sort((a, b) => (Number(a.risk_rank) || 9999) - (Number(b.risk_rank) || 9999))
      .forEach((n, i) => {
        const row = Math.floor(i / maxPerRow);
        const col = i % maxPerRow;
        const rowStart = row * maxPerRow;
        const rowCount = Math.min(maxPerRow, bucket.length - rowStart);
        const dynamicPos = {
          x: minX + ((col + 1) / (rowCount + 1)) * (maxX - minX),
          y: domainBaseY[domain] + (row * rowStep),
        };
        const pos = NODE_POSITIONS[n.service_name] || dynamicPos;
        nodeMap[n.service_name] = { ...n, ...pos };
      });
  });

  // Draw on canvas
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !nodes.length) return;

    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr * zoom, dpr * zoom);

    // Clear
    ctx.fillStyle = '#1c2128';
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    // Draw domain zones
    const zones = [
      { label: 'NETWORK', y: 10, h: 90, color: 'rgba(57, 211, 83, 0.05)', border: 'rgba(57, 211, 83, 0.15)' },
      { label: 'APPLICATION', y: 120, h: 360, color: 'rgba(88, 166, 255, 0.03)', border: 'rgba(88, 166, 255, 0.1)' },
      { label: 'INFRASTRUCTURE', y: 510, h: 200, color: 'rgba(188, 140, 255, 0.05)', border: 'rgba(188, 140, 255, 0.15)' },
    ];
    if (domainBuckets.unknown.length > 0) {
      zones.push({
        label: 'UNKNOWN',
        y: Math.max(10, unknownBaseY - 36),
        h: Math.max(60, ((unknownRows - 1) * rowStep) + 72),
        color: 'rgba(139, 148, 158, 0.05)',
        border: 'rgba(139, 148, 158, 0.2)',
      });
    }

    zones.forEach(z => {
      ctx.fillStyle = z.color;
      ctx.strokeStyle = z.border;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.roundRect(20, z.y, 880, z.h, 8);
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = z.border;
      ctx.font = '600 10px Inter, sans-serif';
      ctx.fillText(z.label, 30, z.y + 16);
    });

    // Draw edges
    edges.forEach(edge => {
      const src = nodeMap[edge.src_service];
      const dst = nodeMap[edge.dst_service];
      if (!src || !dst) return;

      const resets = Number(edge.reset_count) || 0;
      const timeouts = Number(edge.timeout_count) || 0;
      const isAnomalous = resets > 5 || timeouts > 5;

      ctx.beginPath();
      ctx.moveTo(src.x, src.y);
      ctx.lineTo(dst.x, dst.y);
      ctx.strokeStyle = isAnomalous ? 'rgba(248, 81, 73, 0.6)' : 'rgba(48, 54, 61, 0.6)';
      ctx.lineWidth = isAnomalous ? 2 : 1;
      if (isAnomalous) {
        ctx.setLineDash([4, 4]);
      } else {
        ctx.setLineDash([]);
      }
      ctx.stroke();
      ctx.setLineDash([]);
    });

    // Draw nodes
    Object.entries(nodeMap).forEach(([name, node]) => {
      const size = getNodeSize(node);
      const color = getNodeColor(node);
      const isSelected = selectedNode === name;
      const isHovered = hoveredNode === name;

      // Glow for high-risk
      if (Number(node.risk_score) > 200) {
        ctx.beginPath();
        ctx.arc(node.x, node.y, size + 8, 0, Math.PI * 2);
        ctx.fillStyle = `${color}22`;
        ctx.fill();
      }

      // Ring for selected
      if (isSelected || isHovered) {
        ctx.beginPath();
        ctx.arc(node.x, node.y, size + 4, 0, Math.PI * 2);
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      // Node circle
      ctx.beginPath();
      ctx.arc(node.x, node.y, size, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.strokeStyle = `${color}88`;
      ctx.lineWidth = 2;
      ctx.stroke();

      // Incident count inside
      const incCount = Number(node.incident_count_as_root) || 0;
      if (incCount > 0) {
        ctx.fillStyle = '#ffffff';
        ctx.font = `700 ${Math.max(9, size * 0.45)}px Inter, sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(String(incCount), node.x, node.y);
      }

      // Label
      ctx.fillStyle = isSelected || isHovered ? '#e6edf3' : '#8b949e';
      ctx.font = '500 9px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      const shortName = name.replace(/-/g, ' ').replace('service', 'svc');
      ctx.fillText(shortName, node.x, node.y + size + 6);
    });

  }, [nodes, edges, zoom, selectedNode, hoveredNode]);

  // Handle click
  const handleCanvasClick = useCallback((e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = (e.clientX - rect.left) / zoom;
    const y = (e.clientY - rect.top) / zoom;

    let clicked = null;
    Object.entries(nodeMap).forEach(([name, node]) => {
      const dist = Math.sqrt((x - node.x) ** 2 + (y - node.y) ** 2);
      if (dist < getNodeSize(node) + 8) {
        clicked = name;
      }
    });
    setSelectedNode(clicked);
  }, [nodeMap, zoom]);

  // Handle hover
  const handleCanvasMove = useCallback((e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = (e.clientX - rect.left) / zoom;
    const y = (e.clientY - rect.top) / zoom;

    let hovered = null;
    Object.entries(nodeMap).forEach(([name, node]) => {
      const dist = Math.sqrt((x - node.x) ** 2 + (y - node.y) ** 2);
      if (dist < getNodeSize(node) + 8) {
        hovered = name;
      }
    });
    setHoveredNode(hovered);
    canvas.style.cursor = hovered ? 'pointer' : 'default';
  }, [nodeMap, zoom]);

  if (loading) return <LoadingState message="Loading service topology..." />;

  const selectedNodeData = selectedNode ? nodeMap[selectedNode] : null;

  // Get edges for selected node
  const selectedEdges = selectedNode ? edges.filter(
    e => e.src_service === selectedNode || e.dst_service === selectedNode
  ) : [];

  return (
    <div>
      <div className="page-header">
        <h2>Topology Explorer</h2>
        <p className="page-subtitle">
          Service dependency graph with failure propagation visualization
        </p>
      </div>

      <div className="grid-2-1">
        {/* Graph */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '12px 16px', borderBottom: '1px solid var(--color-border)',
          }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              {nodes.length} services | {edges.length} connections
              {' | '}<span style={{ color: '#f85149' }}>Red dashed = anomalous traffic</span>
            </span>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-secondary btn-sm" onClick={() => setZoom(z => Math.min(z + 0.1, 1.5))}>
                <ZoomIn size={14} />
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => setZoom(z => Math.max(z - 0.1, 0.5))}>
                <ZoomOut size={14} />
              </button>
              <button className="btn btn-secondary btn-sm" onClick={() => setZoom(1)}>
                <Maximize2 size={14} />
              </button>
            </div>
          </div>
          <canvas
            ref={canvasRef}
            style={{ width: '100%', height: 720 }}
            onClick={handleCanvasClick}
            onMouseMove={handleCanvasMove}
          />
        </div>

        {/* Detail Panel */}
        <div>
          {selectedNodeData ? (
            <div className="card">
              <div className="card-header">
                <span className="card-title">Service Detail</span>
              </div>
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontWeight: 700, fontSize: '1.1rem', marginBottom: 4 }}>
                  {selectedNode}
                </div>
                <DomainBadge domain={selectedNodeData.domain} />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 20 }}>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Risk Score</div>
                  <div style={{
                    fontSize: '1.5rem', fontWeight: 800,
                    color: Number(selectedNodeData.risk_score) > 500 ? '#f85149' :
                           Number(selectedNodeData.risk_score) > 200 ? '#f0883e' : '#58a6ff',
                  }}>
                    {Number(selectedNodeData.risk_score).toFixed(0)}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Risk Rank</div>
                  <div style={{ fontSize: '1.5rem', fontWeight: 800 }}>#{selectedNodeData.risk_rank}</div>
                </div>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Incidents (Root)</div>
                  <div style={{ fontSize: '1.2rem', fontWeight: 700, color: 'var(--color-critical)' }}>
                    {selectedNodeData.incident_count_as_root}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Health Score</div>
                  <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>
                    {Number(selectedNodeData.avg_health_score).toFixed(0)}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Revenue Impact</div>
                  <div style={{ fontSize: '1rem', fontWeight: 600 }}>
                    {formatCurrency(selectedNodeData.total_revenue_impact)}
                  </div>
                </div>
              </div>

              {/* Connected Services */}
              <div style={{ borderTop: '1px solid var(--color-border)', paddingTop: 12 }}>
                <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--color-text-muted)', marginBottom: 8, textTransform: 'uppercase' }}>
                  Connections ({selectedEdges.length})
                </div>
                {selectedEdges.map((edge, idx) => {
                  const peer = edge.src_service === selectedNode ? edge.dst_service : edge.src_service;
                  const direction = edge.src_service === selectedNode ? 'outbound' : 'inbound';
                  const isAnomalous = (Number(edge.reset_count) > 5) || (Number(edge.timeout_count) > 5);
                  return (
                    <div key={idx} style={{
                      padding: '6px 0',
                      borderBottom: '1px solid var(--color-border-light)',
                      display: 'flex',
                      justifyContent: 'space-between',
                      fontSize: '0.8rem',
                    }}>
                      <div>
                        <code style={{
                          color: isAnomalous ? 'var(--color-critical)' : 'var(--color-text-primary)',
                        }}>
                          {peer}
                        </code>
                        <span style={{ color: 'var(--color-text-muted)', marginLeft: 8, fontSize: '0.7rem' }}>
                          {direction}
                        </span>
                      </div>
                      <div style={{ color: 'var(--color-text-muted)', fontSize: '0.7rem' }}>
                        {Number(edge.flow_count).toLocaleString()} flows
                        {isAnomalous && (
                          <span style={{ color: 'var(--color-critical)', marginLeft: 4 }}>
                            ({edge.reset_count} resets)
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="card" style={{ textAlign: 'center', padding: '48px 24px' }}>
              <Network size={32} style={{ color: 'var(--color-text-muted)', marginBottom: 12 }} />
              <div style={{ color: 'var(--color-text-muted)' }}>
                Click a node to view service details
              </div>
              <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginTop: 8 }}>
                Node size = risk level, Color = domain/severity
              </div>
            </div>
          )}

          {/* Legend */}
          <div className="card" style={{ marginTop: 'var(--spacing-md)' }}>
            <div className="card-header">
              <span className="card-title">Legend</span>
            </div>
            <div style={{ fontSize: '0.8rem', display: 'grid', gap: 8 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#f85149' }} />
                <span>Critical Risk (500+)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#f0883e' }} />
                <span>High Risk (200-500)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#d29922' }} />
                <span>Medium Risk (50-200)</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#bc8cff' }} />
                <span style={{ color: 'var(--color-infrastructure)' }}>Infrastructure</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#58a6ff' }} />
                <span style={{ color: 'var(--color-application)' }}>Application</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 12, height: 12, borderRadius: '50%', background: '#39d353' }} />
                <span style={{ color: 'var(--color-network)' }}>Network</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 20, height: 0, borderTop: '2px dashed #f85149' }} />
                <span>Anomalous Traffic</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
