import React, { useState, useEffect, useMemo } from 'react';
import { Network, ZoomIn, ZoomOut, Maximize2 } from 'lucide-react';
import { useApi, formatCurrency } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';
import { DomainBadge } from '../components/SeverityBadge';

const DOMAIN_COLORS = {
  infrastructure: '#bc8cff',
  application: '#58a6ff',
  network: '#39d353',
  unknown: '#8b949e',
};

// Node positions are computed dynamically per-domain (network/application/infrastructure)
// by the layout logic below. This map can optionally pin specific services to fixed
// coordinates when a curated layout is desired.
const NODE_POSITIONS = {};

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
  const [selectedNode, setSelectedNode] = useState(null);
  const [zoom, setZoom] = useState(1);
  const [hoveredNode, setHoveredNode] = useState(null);
  const [viewMode, setViewMode] = useState('incident');
  const [focusMode, setFocusMode] = useState('drill');
  const [hopDepth, setHopDepth] = useState(1);

  const nodes = topology?.nodes || [];
  const edges = topology?.edges || [];
  const renderedEdges = useMemo(() => (
    viewMode === 'incident'
      ? edges.filter((e) => Number(e.incident_link_count) > 0)
      : edges
  ), [edges, viewMode]);

  const renderedNodeNames = useMemo(() => {
    const names = new Set();
    renderedEdges.forEach((e) => {
      if (e.src_service) names.add(e.src_service);
      if (e.dst_service) names.add(e.dst_service);
    });
    if (names.size === 0 && nodes.length > 0) {
      names.add(nodes[0].service_name);
    }
    return names;
  }, [renderedEdges, nodes]);

  const renderedNodes = useMemo(
    () => nodes.filter((n) => renderedNodeNames.has(n.service_name)),
    [nodes, renderedNodeNames]
  );

  const rootCandidates = useMemo(() => (
    [...renderedNodes]
      .filter((n) => Number(n.incident_count_as_root) > 0)
      .sort((a, b) => {
        const incidentDelta = (Number(b.incident_count_as_root) || 0) - (Number(a.incident_count_as_root) || 0);
        if (incidentDelta !== 0) return incidentDelta;
        return (Number(b.risk_score) || 0) - (Number(a.risk_score) || 0);
      })
      .slice(0, 6)
  ), [renderedNodes]);

  useEffect(() => {
    if (!selectedNode) return;
    if (!renderedNodeNames.has(selectedNode)) {
      setSelectedNode(null);
    }
  }, [selectedNode, renderedNodeNames]);

  // Build node lookup. Use static positions where available, and fall back to
  // a dynamic per-domain layout so new service names still render.
  const nodeMap = {};
  const domainBuckets = {
    network: [],
    application: [],
    infrastructure: [],
    unknown: [],
  };

  renderedNodes.forEach((n) => {
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
  const graphWidth = 920;
  const graphHeight = 720;

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

  const focusNodeNames = useMemo(() => {
    if (!selectedNode || focusMode !== 'drill') return null;
    const visited = new Set([selectedNode]);
    let frontier = [selectedNode];

    for (let level = 0; level < hopDepth; level += 1) {
      const next = [];
      renderedEdges.forEach((edge) => {
        if (frontier.includes(edge.src_service) && !visited.has(edge.dst_service)) {
          visited.add(edge.dst_service);
          next.push(edge.dst_service);
        }
        if (frontier.includes(edge.dst_service) && !visited.has(edge.src_service)) {
          visited.add(edge.src_service);
          next.push(edge.src_service);
        }
      });
      if (next.length === 0) break;
      frontier = next;
    }
    return visited;
  }, [selectedNode, renderedEdges, focusMode, hopDepth]);

  const zones = useMemo(() => {
    const zoneRows = [
      { label: 'NETWORK', y: 10, h: 90, color: 'rgba(57, 211, 83, 0.05)', border: 'rgba(57, 211, 83, 0.15)' },
      { label: 'APPLICATION', y: 120, h: 360, color: 'rgba(88, 166, 255, 0.03)', border: 'rgba(88, 166, 255, 0.1)' },
      { label: 'INFRASTRUCTURE', y: 510, h: 200, color: 'rgba(188, 140, 255, 0.05)', border: 'rgba(188, 140, 255, 0.15)' },
    ];
    if (domainBuckets.unknown.length > 0) {
      zoneRows.push({
        label: 'UNKNOWN',
        y: Math.max(10, unknownBaseY - 36),
        h: Math.max(60, ((unknownRows - 1) * rowStep) + 72),
        color: 'rgba(139, 148, 158, 0.05)',
        border: 'rgba(139, 148, 158, 0.2)',
      });
    }
    return zoneRows;
  }, [domainBuckets.unknown.length, rowStep, unknownBaseY, unknownRows]);

  const graphTransform = `translate(${((1 - zoom) * graphWidth) / 2} ${((1 - zoom) * graphHeight) / 2}) scale(${zoom})`;

  if (loading) return <LoadingState message="Loading service topology..." />;

  const selectedNodeData = selectedNode ? nodeMap[selectedNode] : null;

  // Get edges for selected node
  const selectedEdges = selectedNode ? renderedEdges.filter(
    e => e.src_service === selectedNode || e.dst_service === selectedNode
  ) : [];
  const upstreamEdges = selectedNode
    ? selectedEdges
        .filter((e) => e.dst_service === selectedNode)
        .sort((a, b) => (Number(b.incident_link_count) || 0) - (Number(a.incident_link_count) || 0))
    : [];
  const downstreamEdges = selectedNode
    ? selectedEdges
        .filter((e) => e.src_service === selectedNode)
        .sort((a, b) => (Number(b.incident_link_count) || 0) - (Number(a.incident_link_count) || 0))
    : [];
  const selectedIncidentOutbound = downstreamEdges.reduce((acc, edge) => acc + (Number(edge.incident_link_count) || 0), 0);
  const selectedIncidentInbound = upstreamEdges.reduce((acc, edge) => acc + (Number(edge.incident_link_count) || 0), 0);
  const rootCauseSignal = selectedIncidentOutbound >= selectedIncidentInbound ? 'Likely root / propagator' : 'Likely impacted by upstream dependency';

  return (
    <div>
      <div className="page-header">
        <h2>Topology Explorer</h2>
        <p className="page-subtitle">
          Service dependency graph with failure propagation visualization
        </p>
      </div>

      <div>
        {/* Full-width Graph */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '12px 16px', borderBottom: '1px solid var(--color-border)',
          }}>
            <span style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              {renderedNodes.length} services | {renderedEdges.length} connections
              {' | '}
              {viewMode === 'incident'
                ? <span style={{ color: '#58a6ff' }}>Blue = incident propagation links</span>
                : <span style={{ color: '#f85149' }}>Red dashed = anomalous traffic</span>}
              {selectedNode && focusMode === 'drill' && (
                <>
                  {' | '}
                  <span style={{ color: '#e6edf3' }}>
                    Focused on {selectedNode} ({hopDepth}-hop neighborhood)
                  </span>
                </>
              )}
            </span>
            <div style={{ display: 'flex', gap: 8 }}>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => setViewMode('incident')}
                style={{ background: viewMode === 'incident' ? 'rgba(88, 166, 255, 0.2)' : undefined }}
              >
                Incident View
              </button>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => setViewMode('full')}
                style={{ background: viewMode === 'full' ? 'rgba(88, 166, 255, 0.2)' : undefined }}
              >
                Full View
              </button>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => setFocusMode((m) => (m === 'drill' ? 'all' : 'drill'))}
                style={{ background: focusMode === 'drill' ? 'rgba(88, 166, 255, 0.2)' : undefined }}
              >
                {focusMode === 'drill' ? 'Drill Focus: On' : 'Drill Focus: Off'}
              </button>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => setHopDepth((d) => (d === 1 ? 2 : 1))}
                disabled={focusMode !== 'drill'}
              >
                {hopDepth}-Hop
              </button>
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
          <div style={{
            display: 'flex',
            gap: 8,
            alignItems: 'center',
            flexWrap: 'wrap',
            padding: '10px 16px',
            borderBottom: '1px solid var(--color-border-light)',
            background: 'rgba(13, 17, 23, 0.45)',
          }}>
            <span style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: 0.3 }}>
              Root Cause Candidates
            </span>
            {rootCandidates.length === 0 ? (
              <span style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)' }}>No incident roots in current view</span>
            ) : (
              rootCandidates.map((node) => (
                <button
                  key={node.service_name}
                  className="btn btn-secondary btn-sm"
                  onClick={() => setSelectedNode(node.service_name)}
                  style={{
                    background: selectedNode === node.service_name ? 'rgba(88, 166, 255, 0.2)' : undefined,
                    borderColor: selectedNode === node.service_name ? 'rgba(88, 166, 255, 0.35)' : undefined,
                  }}
                >
                  {node.service_name} ({node.incident_count_as_root})
                </button>
              ))
            )}
          </div>
          {selectedNodeData ? (
            <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--color-border-light)', background: 'rgba(22, 27, 34, 0.7)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: 0.3 }}>
                    Service Detail
                  </span>
                  <strong>{selectedNode}</strong>
                  <DomainBadge domain={selectedNodeData.domain} />
                </div>
                <button className="btn btn-secondary btn-sm" onClick={() => setSelectedNode(null)}>
                  Close
                </button>
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, minmax(0,1fr))', gap: 8, marginBottom: 10 }}>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Risk</span><br /><strong>{Number(selectedNodeData.risk_score).toFixed(0)}</strong></div>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Rank</span><br /><strong>#{selectedNodeData.risk_rank}</strong></div>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Root Incidents</span><br /><strong>{selectedNodeData.incident_count_as_root}</strong></div>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Inbound Links</span><br /><strong>{selectedIncidentInbound}</strong></div>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Outbound Links</span><br /><strong>{selectedIncidentOutbound}</strong></div>
                <div style={{ fontSize: '0.78rem' }}><span style={{ color: 'var(--color-text-muted)' }}>Revenue</span><br /><strong>{formatCurrency(selectedNodeData.total_revenue_impact)}</strong></div>
              </div>

              <div style={{ fontSize: '0.78rem', color: 'var(--color-text-muted)', marginBottom: 8 }}>
                Directional read: incident links flow from source to target (left to right arrows). <strong>{rootCauseSignal}</strong>
              </div>

              <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
                <div style={{ minWidth: 280, flex: 1 }}>
                  <div style={{ fontSize: '0.72rem', textTransform: 'uppercase', color: 'var(--color-text-muted)', marginBottom: 6 }}>
                    Upstream Contributors ({upstreamEdges.length})
                  </div>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {upstreamEdges.slice(0, 5).map((edge, idx) => (
                      <button key={`up-${idx}`} className="btn btn-secondary btn-sm" style={{ padding: '2px 8px' }} onClick={() => setSelectedNode(edge.src_service)}>
                        {edge.src_service}
                      </button>
                    ))}
                  </div>
                </div>
                <div style={{ minWidth: 280, flex: 1 }}>
                  <div style={{ fontSize: '0.72rem', textTransform: 'uppercase', color: 'var(--color-text-muted)', marginBottom: 6 }}>
                    Downstream Blast Radius ({downstreamEdges.length})
                  </div>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {downstreamEdges.slice(0, 5).map((edge, idx) => (
                      <button key={`down-${idx}`} className="btn btn-secondary btn-sm" style={{ padding: '2px 8px' }} onClick={() => setSelectedNode(edge.dst_service)}>
                        {edge.dst_service}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div style={{ padding: '10px 16px', borderBottom: '1px solid var(--color-border-light)', color: 'var(--color-text-muted)', fontSize: '0.78rem' }}>
              Click a node to inspect service detail; incident arrows show source (potential root) to impacted services.
            </div>
          )}

          <div style={{ position: 'relative' }}>
            <svg
              viewBox={`0 0 ${graphWidth} ${graphHeight}`}
              style={{ width: '100%', height: 720, display: 'block', background: '#1c2128' }}
              onMouseLeave={() => setHoveredNode(null)}
              onClick={(e) => {
                if (e.target === e.currentTarget) setSelectedNode(null);
              }}
            >
              <defs>
                <marker id="arrow-blue" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                  <path d="M0,0 L8,4 L0,8 Z" fill="rgba(88, 166, 255, 0.9)" />
                </marker>
                <marker id="arrow-red" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                  <path d="M0,0 L8,4 L0,8 Z" fill="rgba(248, 81, 73, 0.8)" />
                </marker>
                <marker id="arrow-gray" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
                  <path d="M0,0 L8,4 L0,8 Z" fill="rgba(139, 148, 158, 0.65)" />
                </marker>
              </defs>
              <g transform={graphTransform}>
              {zones.map((z) => (
                <g key={z.label}>
                  <rect
                    x={20}
                    y={z.y}
                    width={880}
                    height={z.h}
                    rx={8}
                    fill={z.color}
                    stroke={z.border}
                  />
                  <text
                    x={30}
                    y={z.y + 16}
                    fill={z.border}
                    fontSize={10}
                    fontWeight={600}
                  >
                    {z.label}
                  </text>
                </g>
              ))}

              {renderedEdges.map((edge) => {
                const src = nodeMap[edge.src_service];
                const dst = nodeMap[edge.dst_service];
                if (!src || !dst) return null;

                const inFocus = !focusNodeNames || (focusNodeNames.has(edge.src_service) && focusNodeNames.has(edge.dst_service));
                const incidentLinks = Number(edge.incident_link_count) || 0;
                const resets = Number(edge.reset_count) || 0;
                const timeouts = Number(edge.timeout_count) || 0;
                const isAnomalous = resets > 5 || timeouts > 5;
                const isIncidentEdge = incidentLinks > 0;
                const touchesSelection = selectedNode && (edge.src_service === selectedNode || edge.dst_service === selectedNode);

                let stroke = 'rgba(48, 54, 61, 0.6)';
                let strokeWidth = 1;
                let strokeDasharray = '';
                let markerEnd = 'url(#arrow-gray)';
                if (!inFocus) {
                  stroke = 'rgba(48, 54, 61, 0.2)';
                } else if (isIncidentEdge) {
                  stroke = touchesSelection ? 'rgba(88, 166, 255, 1)' : 'rgba(88, 166, 255, 0.78)';
                  strokeWidth = Math.min(5, 1.2 + incidentLinks * 0.6);
                  markerEnd = 'url(#arrow-blue)';
                } else if (isAnomalous) {
                  stroke = touchesSelection ? 'rgba(248, 81, 73, 0.85)' : 'rgba(248, 81, 73, 0.55)';
                  strokeWidth = touchesSelection ? 2.4 : 1.8;
                  strokeDasharray = '4 4';
                  markerEnd = 'url(#arrow-red)';
                } else if (touchesSelection) {
                  stroke = 'rgba(139, 148, 158, 0.75)';
                }

                return (
                  <line
                    key={`${edge.src_service}->${edge.dst_service}`}
                    x1={src.x}
                    y1={src.y}
                    x2={dst.x}
                    y2={dst.y}
                    stroke={stroke}
                    strokeWidth={strokeWidth}
                    strokeDasharray={strokeDasharray}
                    markerEnd={markerEnd}
                  />
                );
              })}

              {Object.entries(nodeMap).map(([name, node]) => {
                const size = getNodeSize(node);
                const inFocus = !focusNodeNames || focusNodeNames.has(name);
                const color = inFocus ? getNodeColor(node) : '#30363d';
                const isSelected = selectedNode === name;
                const isHovered = hoveredNode === name;
                const incidentCount = Number(node.incident_count_as_root) || 0;
                const shortName = name.replace(/-/g, ' ').replace('service', 'svc');

                return (
                  <g
                    key={name}
                    onMouseEnter={() => setHoveredNode(name)}
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedNode(name);
                    }}
                    style={{ cursor: 'pointer' }}
                  >
                    {inFocus && Number(node.risk_score) > 200 && (
                      <circle cx={node.x} cy={node.y} r={size + 8} fill={`${color}22`} />
                    )}
                    {(isSelected || isHovered) && (
                      <circle
                        cx={node.x}
                        cy={node.y}
                        r={size + 4}
                        fill="transparent"
                        stroke="#ffffff"
                        strokeWidth={2}
                      />
                    )}
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={size}
                      fill={color}
                      stroke={`${color}88`}
                      strokeWidth={2}
                    />
                    {incidentCount > 0 && (
                      <text
                        x={node.x}
                        y={node.y}
                        fill="#ffffff"
                        fontSize={Math.max(9, size * 0.45)}
                        fontWeight={700}
                        textAnchor="middle"
                        dominantBaseline="middle"
                      >
                        {incidentCount}
                      </text>
                    )}
                    <text
                      x={node.x}
                      y={node.y + size + 6}
                      fill={isSelected || isHovered ? '#e6edf3' : (inFocus ? '#8b949e' : '#6e7681')}
                      fontSize={9}
                      fontWeight={500}
                      textAnchor="middle"
                      dominantBaseline="hanging"
                    >
                      {shortName}
                    </text>
                  </g>
                );
              })}
              </g>
            </svg>
          </div>
        </div>

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
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ width: 20, height: 0, borderTop: '3px solid #58a6ff', position: 'relative' }} />
              <span>Incident Propagation (source → impacted)</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
