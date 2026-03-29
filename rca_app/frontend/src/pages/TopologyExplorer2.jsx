import React, { useMemo, useCallback, useState } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  ReactFlowProvider,
  MarkerType,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Network, X } from 'lucide-react';
import { useApi, formatCurrency } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';

const ACCENT = '#00d4ff';
const DANGER = '#f85149';
const WARN = '#f0883e';

function riskColor(score) {
  const s = Number(score) || 0;
  if (s > 500) return DANGER;
  if (s > 200) return WARN;
  return ACCENT;
}

function displayName(name) {
  return (name || '').replace(/-/g, ' ');
}

/* ── Custom Node Component ────────────────────────────────────── */
function ServiceNode({ data }) {
  const risk = Number(data.risk_score) || 0;
  const incidents = Number(data.incident_count_as_root) || 0;
  const color = riskColor(risk);

  return (
    <div style={{ position: 'relative' }}>
      <Handle type="target" position={Position.Top}
        style={{ background: color, border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="target" position={Position.Left} id="left-in"
        style={{ background: color, border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="source" position={Position.Bottom}
        style={{ background: color, border: 'none', width: 6, height: 6, opacity: 0 }} />
      <Handle type="source" position={Position.Right} id="right-out"
        style={{ background: color, border: 'none', width: 6, height: 6, opacity: 0 }} />

      <div style={{
        width: 90,
        height: 90,
        borderRadius: '50%',
        background: 'rgba(13, 17, 23, 0.95)',
        border: `2.5px solid ${color}`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        textAlign: 'center',
        padding: 10,
        boxShadow: risk > 200 ? `0 0 12px ${color}20` : 'none',
        transition: 'all 0.2s ease',
      }}>
        <span style={{
          color: '#9ca3af',
          fontSize: 10.5,
          fontWeight: 500,
          lineHeight: 1.25,
          wordBreak: 'break-word',
          letterSpacing: 0.2,
        }}>
          {displayName(data.label)}
        </span>
      </div>

      {incidents > 0 && (
        <div style={{
          position: 'absolute',
          top: -4,
          right: -4,
          minWidth: 20,
          height: 20,
          borderRadius: 10,
          background: color,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 9,
          fontWeight: 700,
          color: '#fff',
          border: '2px solid #0d1117',
          padding: '0 4px',
        }}>
          {incidents}
        </div>
      )}
    </div>
  );
}

const nodeTypes = { service: ServiceNode };

/* ── Force layout ─────────────────────────────────────────────── */
function computeLayout(apiNodes, apiEdges) {
  const W = 1400;
  const H = 900;

  if (!apiNodes || apiNodes.length === 0) return { flowNodes: [], flowEdges: [] };

  const incidentEdges = (apiEdges || []).filter((e) => Number(e.incident_link_count) > 0);
  const active = incidentEdges.length > 0 ? incidentEdges : (apiEdges || []).slice(0, 30);

  const names = new Set();
  active.forEach((e) => { names.add(e.src_service); names.add(e.dst_service); });
  const filtered = apiNodes.filter((n) => names.has(n.service_name));

  if (filtered.length === 0) return { flowNodes: [], flowEdges: [] };

  const k = Math.sqrt((W * H) / filtered.length) * 0.5;
  const pos = {};
  filtered.forEach((n, i) => {
    const angle = (2 * Math.PI * i) / filtered.length;
    const r = Math.min(W, H) * 0.3;
    pos[n.service_name] = {
      x: W / 2 + r * Math.cos(angle),
      y: H / 2 + r * Math.sin(angle),
      vx: 0, vy: 0,
    };
  });

  for (let iter = 0; iter < 250; iter++) {
    const temp = 0.12 * (1 - iter / 250);
    for (let i = 0; i < filtered.length; i++) {
      for (let j = i + 1; j < filtered.length; j++) {
        const a = pos[filtered[i].service_name];
        const b = pos[filtered[j].service_name];
        const dx = (a.x - b.x) || 0.1;
        const dy = (a.y - b.y) || 0.1;
        const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
        const f = (k * k) / dist * temp;
        a.vx += (dx / dist) * f; a.vy += (dy / dist) * f;
        b.vx -= (dx / dist) * f; b.vy -= (dy / dist) * f;
      }
    }
    active.forEach((e) => {
      const a = pos[e.src_service];
      const b = pos[e.dst_service];
      if (!a || !b) return;
      const dx = (b.x - a.x) || 0.1;
      const dy = (b.y - a.y) || 0.1;
      const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
      const f = (dist * dist) / k * temp;
      a.vx += (dx / dist) * f; a.vy += (dy / dist) * f;
      b.vx -= (dx / dist) * f; b.vy -= (dy / dist) * f;
    });
    filtered.forEach((n) => {
      const p = pos[n.service_name];
      p.vx += (W / 2 - p.x) * 0.01;
      p.vy += (H / 2 - p.y) * 0.01;
      p.x = Math.max(60, Math.min(W - 60, p.x + p.vx));
      p.y = Math.max(60, Math.min(H - 60, p.y + p.vy));
      p.vx = 0; p.vy = 0;
    });
  }

  const flowNodes = filtered.map((n) => ({
    id: n.service_name,
    type: 'service',
    position: { x: pos[n.service_name].x - 45, y: pos[n.service_name].y - 45 },
    data: {
      label: n.service_name,
      risk_score: n.risk_score,
      risk_rank: n.risk_rank,
      incident_count_as_root: n.incident_count_as_root,
      avg_health_score: n.avg_health_score,
      total_revenue_impact: n.total_revenue_impact,
      domain: n.domain,
    },
  }));

  const flowEdges = active
    .filter((e) => pos[e.src_service] && pos[e.dst_service])
    .map((e, i) => ({
      id: `e-${i}-${e.src_service}-${e.dst_service}`,
      source: e.src_service,
      target: e.dst_service,
      animated: Number(e.incident_link_count) > 0,
      type: 'smoothstep',
      style: {
        stroke: ACCENT,
        strokeWidth: Math.min(4, 1.5 + (Number(e.incident_link_count) || 0) * 0.5),
      },
      markerEnd: { type: MarkerType.ArrowClosed, color: ACCENT, width: 16, height: 16 },
    }));

  return { flowNodes, flowEdges };
}

/* ── Inner Flow (must be inside ReactFlowProvider) ────────────── */
function FlowGraph({ flowNodes, flowEdges, onSelectNode }) {
  const [nodes, setNodes] = useState(flowNodes);
  const [edges] = useState(flowEdges);

  const onNodesChange = useCallback((changes) => {
    setNodes((nds) => {
      const updated = [...nds];
      changes.forEach((change) => {
        if (change.type === 'position' && change.position) {
          const idx = updated.findIndex((n) => n.id === change.id);
          if (idx >= 0) {
            updated[idx] = { ...updated[idx], position: change.position };
          }
        }
      });
      return updated;
    });
  }, []);

  const handleNodeClick = useCallback((_, node) => {
    onSelectNode(node.id);
  }, [onSelectNode]);

  const handlePaneClick = useCallback(() => {
    onSelectNode(null);
  }, [onSelectNode]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onNodeClick={handleNodeClick}
      onPaneClick={handlePaneClick}
      nodeTypes={nodeTypes}
      fitView
      fitViewOptions={{ padding: 0.2 }}
      minZoom={0.2}
      maxZoom={3}
      proOptions={{ hideAttribution: true }}
    >
      <Background color="#1c2128" gap={20} size={1} />
      <Controls showInteractive={false} position="top-right" />
      <MiniMap
        nodeColor={(n) => riskColor(n.data?.risk_score)}
        maskColor="rgba(13,17,23,0.85)"
        style={{ background: '#161b22', border: '1px solid #30363d', borderRadius: 8 }}
        pannable
        zoomable
      />
    </ReactFlow>
  );
}

/* ── Main page ────────────────────────────────────────────────── */
export default function TopologyExplorer2() {
  const { data: topology, loading } = useApi('/api/services/topology');
  const [selectedNodeId, setSelectedNodeId] = useState(null);

  const apiNodes = topology?.nodes || [];
  const apiEdges = topology?.edges || [];

  const { flowNodes, flowEdges } = useMemo(
    () => computeLayout(apiNodes, apiEdges),
    [apiNodes, apiEdges],
  );

  const selectedData = selectedNodeId
    ? apiNodes.find((n) => n.service_name === selectedNodeId)
    : null;

  const selectedEdgeList = selectedNodeId
    ? apiEdges.filter(
        (e) =>
          (e.src_service === selectedNodeId || e.dst_service === selectedNodeId) &&
          Number(e.incident_link_count) > 0,
      )
    : [];

  if (loading) return <LoadingState message="Loading service topology..." />;

  if (flowNodes.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h2>Service Map</h2>
          <p className="page-subtitle">No topology data available</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <h2 style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Network size={22} style={{ color: ACCENT }} />
          Service Map
        </h2>
        <p className="page-subtitle">
          Interactive service dependency graph — drag nodes, scroll to zoom
        </p>
      </div>

      <div style={{ display: 'flex', gap: 'var(--spacing-md)', alignItems: 'flex-start' }}>
        <div className="card" style={{ flex: 1, padding: 0, overflow: 'hidden' }}>
          <div style={{ width: '100%', height: 700 }}>
            <ReactFlowProvider>
              <FlowGraph
                flowNodes={flowNodes}
                flowEdges={flowEdges}
                onSelectNode={setSelectedNodeId}
              />
            </ReactFlowProvider>
          </div>
        </div>

        {selectedData && (
          <div className="card" style={{ width: 300, flexShrink: 0 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h3 style={{ margin: 0, fontSize: '0.95rem' }}>{displayName(selectedNodeId)}</h3>
              <button className="btn btn-secondary btn-sm" onClick={() => setSelectedNodeId(null)} style={{ padding: '4px 6px' }}>
                <X size={14} />
              </button>
            </div>

            <div style={{
              display: 'inline-block', padding: '2px 8px', borderRadius: 12, fontSize: '0.72rem',
              fontWeight: 600, background: 'rgba(0,212,255,0.1)', color: ACCENT,
              border: '1px solid rgba(0,212,255,0.25)', marginBottom: 16, textTransform: 'capitalize',
            }}>
              {selectedData.domain || 'unknown'}
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 20 }}>
              <MetricCard label="Risk Score" value={Number(selectedData.risk_score).toFixed(0)} color={riskColor(selectedData.risk_score)} />
              <MetricCard label="Risk Rank" value={`#${selectedData.risk_rank}`} />
              <MetricCard label="Root Incidents" value={selectedData.incident_count_as_root} />
              <MetricCard label="Health" value={`${Number(selectedData.avg_health_score).toFixed(0)}%`} />
              <MetricCard label="Revenue Impact" value={formatCurrency(selectedData.total_revenue_impact)} style={{ gridColumn: 'span 2' }} />
            </div>

            <div>
              <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', marginBottom: 8, letterSpacing: 0.5 }}>
                Connections ({selectedEdgeList.length})
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 250, overflowY: 'auto' }}>
                {selectedEdgeList.map((edge, i) => {
                  const other = edge.src_service === selectedNodeId ? edge.dst_service : edge.src_service;
                  const dir = edge.src_service === selectedNodeId ? '→' : '←';
                  return (
                    <button key={i} className="btn btn-secondary btn-sm"
                      onClick={() => setSelectedNodeId(other)}
                      style={{ textAlign: 'left', display: 'flex', justifyContent: 'space-between', fontSize: '0.78rem', padding: '6px 10px' }}>
                      <span>{displayName(other)}</span>
                      <span style={{ fontSize: '0.68rem', color: 'var(--color-text-muted)' }}>
                        {dir} {Number(edge.incident_link_count) || 0} incidents
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function MetricCard({ label, value, color, style }) {
  return (
    <div style={{ background: 'var(--color-bg-secondary)', borderRadius: 8, padding: '10px 12px', ...style }}>
      <div style={{ fontSize: '0.68rem', color: 'var(--color-text-muted)', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: '1.05rem', fontWeight: 700, color: color || 'var(--color-text-primary)' }}>{value}</div>
    </div>
  );
}
