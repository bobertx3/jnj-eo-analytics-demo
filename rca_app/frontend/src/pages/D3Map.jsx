import React, { useRef, useEffect, useState, useMemo } from 'react';
import {
  forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide,
} from 'd3-force';
import { select } from 'd3-selection';
import { drag as d3Drag } from 'd3-drag';
import { zoom as d3Zoom } from 'd3-zoom';
import { Network, X } from 'lucide-react';
import { useApi, formatCurrency } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';

const ACCENT = '#00d4ff';
const DANGER = '#f85149';
const WARN = '#f0883e';
const MEDIUM = '#d29922';
const BG = '#0d1117';
const BORDER = '#30363d';
const TEXT = '#9ca3af';
const TEXT_DIM = '#6b7280';

const DOMAIN_COLORS = {
  infrastructure: '#bc8cff',
  application: '#58a6ff',
  network: '#39d353',
  unknown: '#8b949e',
};

function riskColor(score) {
  const s = Number(score) || 0;
  if (s > 500) return DANGER;
  if (s > 200) return WARN;
  if (s > 50) return MEDIUM;
  return ACCENT;
}

function nodeStrokeColor(node) {
  const risk = Number(node.risk_score) || 0;
  if (risk > 500) return DANGER;
  if (risk > 200) return WARN;
  if (risk > 50) return MEDIUM;
  return DOMAIN_COLORS[node.domain] || '#8b949e';
}

function displayName(name) {
  return (name || '').replace(/-/g, ' ');
}

export default function D3Map() {
  const { data: topology, loading } = useApi('/api/services/topology');
  const svgRef = useRef(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [viewMode, setViewMode] = useState('incident');
  const [dimensions, setDimensions] = useState({ width: 1000, height: 650 });
  const containerRef = useRef(null);

  // Responsive sizing
  useEffect(() => {
    function handleResize() {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect();
        setDimensions({ width: rect.width, height: Math.max(650, rect.height) });
      }
    }
    handleResize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  const apiNodes = topology?.nodes || [];
  const apiEdges = topology?.edges || [];

  const activeEdges = useMemo(() => {
    if (viewMode === 'incident') {
      const incEdges = apiEdges.filter((e) => Number(e.incident_link_count) > 0);
      return incEdges.length > 0 ? incEdges : apiEdges.slice(0, 30);
    }
    return apiEdges;
  }, [apiEdges, viewMode]);

  const filteredNodes = useMemo(() => {
    const names = new Set();
    activeEdges.forEach((e) => { names.add(e.src_service); names.add(e.dst_service); });
    return apiNodes.filter((n) => names.has(n.service_name));
  }, [apiNodes, activeEdges]);

  // Root cause candidates
  const rootCandidates = useMemo(() => (
    [...filteredNodes]
      .filter((n) => Number(n.incident_count_as_root) > 0)
      .sort((a, b) => {
        const d = (Number(b.incident_count_as_root) || 0) - (Number(a.incident_count_as_root) || 0);
        return d !== 0 ? d : (Number(b.risk_score) || 0) - (Number(a.risk_score) || 0);
      })
      .slice(0, 6)
  ), [filteredNodes]);

  // D3 rendering
  useEffect(() => {
    if (!svgRef.current || filteredNodes.length === 0) return;

    const { width, height } = dimensions;

    const nodes = filteredNodes.map((n) => ({ id: n.service_name, ...n }));
    const nodeIndex = {};
    nodes.forEach((n, i) => { nodeIndex[n.id] = i; });

    const links = activeEdges
      .filter((e) => nodeIndex[e.src_service] !== undefined && nodeIndex[e.dst_service] !== undefined)
      .map((e) => ({
        source: e.src_service,
        target: e.dst_service,
        incident_link_count: Number(e.incident_link_count) || 0,
      }));

    const svg = select(svgRef.current);
    svg.selectAll('*').remove();

    // Defs
    const defs = svg.append('defs');
    [ACCENT, DANGER, WARN, '#58a6ff', '#30363d'].forEach((color) => {
      defs.append('marker')
        .attr('id', `arrow-${color.replace('#', '')}`)
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 52)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', color)
        .attr('opacity', 0.7);
    });

    // Glow filter
    const filter = defs.append('filter').attr('id', 'glow');
    filter.append('feGaussianBlur').attr('stdDeviation', '3').attr('result', 'coloredBlur');
    const feMerge = filter.append('feMerge');
    feMerge.append('feMergeNode').attr('in', 'coloredBlur');
    feMerge.append('feMergeNode').attr('in', 'SourceGraphic');

    const g = svg.append('g');

    // Zoom
    const zoomBehavior = d3Zoom()
      .scaleExtent([0.2, 4])
      .on('zoom', (event) => { g.attr('transform', event.transform); });
    svg.call(zoomBehavior);

    // Links
    const link = g.append('g')
      .selectAll('line')
      .data(links)
      .join('line')
      .attr('stroke', (d) => {
        if (d.incident_link_count > 3) return DANGER;
        if (d.incident_link_count > 0) return '#58a6ff';
        return BORDER;
      })
      .attr('stroke-opacity', (d) => d.incident_link_count > 0 ? 0.65 : 0.2)
      .attr('stroke-width', (d) => Math.min(5, 1.2 + d.incident_link_count * 0.6))
      .attr('marker-end', (d) => {
        if (d.incident_link_count > 3) return `url(#arrow-${DANGER.replace('#', '')})`;
        if (d.incident_link_count > 0) return `url(#arrow-58a6ff)`;
        return `url(#arrow-30363d)`;
      });

    // Node groups
    const node = g.append('g')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .style('cursor', 'pointer')
      .on('click', (event, d) => {
        event.stopPropagation();
        setSelectedNode(d);
      });

    // Outer glow ring for high-risk
    node.filter((d) => Number(d.risk_score) > 200)
      .append('circle')
      .attr('r', 48)
      .attr('fill', (d) => `${nodeStrokeColor(d)}15`)
      .attr('stroke', 'none');

    // Node circles — sized by risk
    node.append('circle')
      .attr('r', (d) => {
        const risk = Number(d.risk_score) || 0;
        if (risk > 500) return 42;
        if (risk > 200) return 38;
        return 34;
      })
      .attr('fill', BG)
      .attr('stroke', (d) => nodeStrokeColor(d))
      .attr('stroke-width', 2.5)
      .attr('filter', (d) => Number(d.risk_score) > 200 ? 'url(#glow)' : null);

    // Node labels
    node.append('text')
      .text((d) => displayName(d.service_name))
      .attr('text-anchor', 'middle')
      .attr('dy', '0.35em')
      .attr('fill', TEXT)
      .attr('font-size', '9px')
      .attr('font-weight', 500)
      .attr('pointer-events', 'none')
      .each(function (d) {
        const self = select(this);
        const words = displayName(d.service_name).split(' ');
        if (words.length > 1) {
          self.text('');
          const half = Math.ceil(words.length / 2);
          self.append('tspan').attr('x', 0).attr('dy', '-0.4em').text(words.slice(0, half).join(' '));
          self.append('tspan').attr('x', 0).attr('dy', '1.1em').text(words.slice(half).join(' '));
        }
      });

    // Incident count badges
    node.filter((d) => Number(d.incident_count_as_root) > 0)
      .append('circle')
      .attr('cx', 28).attr('cy', -28).attr('r', 10)
      .attr('fill', (d) => riskColor(d.risk_score))
      .attr('stroke', BG).attr('stroke-width', 2);

    node.filter((d) => Number(d.incident_count_as_root) > 0)
      .append('text')
      .attr('x', 28).attr('y', -28)
      .attr('text-anchor', 'middle').attr('dy', '0.35em')
      .attr('fill', '#fff').attr('font-size', '8px').attr('font-weight', 700)
      .attr('pointer-events', 'none')
      .text((d) => d.incident_count_as_root);

    // Domain label below node
    node.append('text')
      .attr('text-anchor', 'middle')
      .attr('dy', (d) => {
        const r = Number(d.risk_score) > 500 ? 42 : Number(d.risk_score) > 200 ? 38 : 34;
        return r + 14;
      })
      .attr('fill', (d) => DOMAIN_COLORS[d.domain] || '#8b949e')
      .attr('font-size', '7px')
      .attr('font-weight', 600)
      .attr('pointer-events', 'none')
      .attr('opacity', 0.6)
      .text((d) => (d.domain || 'unknown').toUpperCase());

    // Drag
    const dragBehavior = d3Drag()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null; d.fy = null;
      });
    node.call(dragBehavior);

    svg.on('click', () => setSelectedNode(null));

    // Force simulation
    const simulation = forceSimulation(nodes)
      .force('link', forceLink(links).id((d) => d.id).distance(150))
      .force('charge', forceManyBody().strength(-400))
      .force('center', forceCenter(width / 2, height / 2))
      .force('collide', forceCollide(60))
      .on('tick', () => {
        link
          .attr('x1', (d) => d.source.x).attr('y1', (d) => d.source.y)
          .attr('x2', (d) => d.target.x).attr('y2', (d) => d.target.y);
        node.attr('transform', (d) => `translate(${d.x},${d.y})`);
      });

    return () => { simulation.stop(); };
  }, [filteredNodes, activeEdges, dimensions]);

  // Compute upstream/downstream for selected node
  const upstreamEdges = useMemo(() => {
    if (!selectedNode) return [];
    return apiEdges
      .filter((e) => e.dst_service === selectedNode.service_name && Number(e.incident_link_count) > 0)
      .sort((a, b) => (Number(b.incident_link_count) || 0) - (Number(a.incident_link_count) || 0));
  }, [selectedNode, apiEdges]);

  const downstreamEdges = useMemo(() => {
    if (!selectedNode) return [];
    return apiEdges
      .filter((e) => e.src_service === selectedNode.service_name && Number(e.incident_link_count) > 0)
      .sort((a, b) => (Number(b.incident_link_count) || 0) - (Number(a.incident_link_count) || 0));
  }, [selectedNode, apiEdges]);

  const selectedIncidentInbound = upstreamEdges.reduce((acc, e) => acc + (Number(e.incident_link_count) || 0), 0);
  const selectedIncidentOutbound = downstreamEdges.reduce((acc, e) => acc + (Number(e.incident_link_count) || 0), 0);
  const rootCauseSignal = selectedIncidentOutbound >= selectedIncidentInbound
    ? 'Likely root / propagator'
    : 'Likely impacted by upstream dependency';

  if (loading) return <LoadingState message="Loading service topology..." />;

  if (apiNodes.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h2>Service Map Explorer</h2>
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
          Service Map Explorer
        </h2>
        <p className="page-subtitle">
          Force-directed service graph — drag nodes, scroll to zoom, pan to navigate
        </p>
      </div>

      {/* Graph Card */}
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        {/* Toolbar */}
        <div style={{
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          padding: '10px 16px', borderBottom: '1px solid var(--color-border)',
        }}>
          <span style={{ fontSize: '0.8rem', color: TEXT_DIM }}>
            {filteredNodes.length} services | {activeEdges.length} connections
            {viewMode === 'incident'
              ? <span style={{ color: '#58a6ff' }}> | Incident propagation links</span>
              : <span style={{ color: TEXT }}> | All network connections</span>}
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
          </div>
        </div>

        {/* Root Cause Candidates Bar */}
        <div style={{
          display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap',
          padding: '8px 16px', borderBottom: '1px solid var(--color-border-light)',
          background: 'rgba(13, 17, 23, 0.45)',
        }}>
          <span style={{ fontSize: '0.72rem', color: TEXT_DIM, textTransform: 'uppercase', letterSpacing: 0.3 }}>
            Root Cause Candidates
          </span>
          {rootCandidates.length === 0 ? (
            <span style={{ fontSize: '0.75rem', color: TEXT_DIM }}>No incident roots in current view</span>
          ) : (
            rootCandidates.map((node) => (
              <button
                key={node.service_name}
                className="btn btn-secondary btn-sm"
                onClick={() => {
                  const match = apiNodes.find((n) => n.service_name === node.service_name);
                  if (match) setSelectedNode({ id: match.service_name, ...match });
                }}
                style={{
                  background: selectedNode?.service_name === node.service_name ? 'rgba(88, 166, 255, 0.2)' : undefined,
                  borderColor: selectedNode?.service_name === node.service_name ? 'rgba(88, 166, 255, 0.35)' : undefined,
                }}
              >
                {node.service_name} ({node.incident_count_as_root})
              </button>
            ))
          )}
        </div>

        {/* Service Detail Panel (inline, above graph) */}
        {selectedNode ? (
          <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--color-border-light)', background: 'rgba(22, 27, 34, 0.7)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: '0.75rem', color: TEXT_DIM, textTransform: 'uppercase', letterSpacing: 0.3 }}>Service Detail</span>
                <strong style={{ color: '#e6edf3' }}>{selectedNode.service_name}</strong>
                <span style={{
                  display: 'inline-block', padding: '1px 8px', borderRadius: 12, fontSize: '0.7rem', fontWeight: 600,
                  background: `${DOMAIN_COLORS[selectedNode.domain] || '#8b949e'}18`,
                  color: DOMAIN_COLORS[selectedNode.domain] || '#8b949e',
                  border: `1px solid ${DOMAIN_COLORS[selectedNode.domain] || '#8b949e'}40`,
                  textTransform: 'capitalize',
                }}>
                  {selectedNode.domain || 'unknown'}
                </span>
              </div>
              <button className="btn btn-secondary btn-sm" onClick={() => setSelectedNode(null)} style={{ padding: '4px 6px' }}>
                <X size={14} />
              </button>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, minmax(0,1fr))', gap: 8, marginBottom: 10 }}>
              <MiniStat label="Risk Score" value={Number(selectedNode.risk_score).toFixed(0)} color={riskColor(selectedNode.risk_score)} />
              <MiniStat label="Risk Rank" value={`#${selectedNode.risk_rank}`} />
              <MiniStat label="Root Incidents" value={selectedNode.incident_count_as_root} />
              <MiniStat label="Inbound Links" value={selectedIncidentInbound} />
              <MiniStat label="Outbound Links" value={selectedIncidentOutbound} />
              <MiniStat label="Revenue Impact" value={formatCurrency(selectedNode.total_revenue_impact)} />
            </div>

            <div style={{ fontSize: '0.78rem', color: TEXT_DIM, marginBottom: 8 }}>
              Incident links flow from source to target. <strong style={{ color: '#e6edf3' }}>{rootCauseSignal}</strong>
            </div>

            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
              <div style={{ minWidth: 260, flex: 1 }}>
                <div style={{ fontSize: '0.72rem', textTransform: 'uppercase', color: TEXT_DIM, marginBottom: 6 }}>
                  Upstream Contributors ({upstreamEdges.length})
                </div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {upstreamEdges.slice(0, 5).map((edge, i) => (
                    <button key={i} className="btn btn-secondary btn-sm" style={{ padding: '2px 8px' }}
                      onClick={() => {
                        const match = apiNodes.find((n) => n.service_name === edge.src_service);
                        if (match) setSelectedNode({ id: match.service_name, ...match });
                      }}>
                      {edge.src_service}
                    </button>
                  ))}
                  {upstreamEdges.length === 0 && <span style={{ fontSize: '0.75rem', color: TEXT_DIM }}>None</span>}
                </div>
              </div>
              <div style={{ minWidth: 260, flex: 1 }}>
                <div style={{ fontSize: '0.72rem', textTransform: 'uppercase', color: TEXT_DIM, marginBottom: 6 }}>
                  Downstream Blast Radius ({downstreamEdges.length})
                </div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {downstreamEdges.slice(0, 5).map((edge, i) => (
                    <button key={i} className="btn btn-secondary btn-sm" style={{ padding: '2px 8px' }}
                      onClick={() => {
                        const match = apiNodes.find((n) => n.service_name === edge.dst_service);
                        if (match) setSelectedNode({ id: match.service_name, ...match });
                      }}>
                      {edge.dst_service}
                    </button>
                  ))}
                  {downstreamEdges.length === 0 && <span style={{ fontSize: '0.75rem', color: TEXT_DIM }}>None</span>}
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div style={{ padding: '8px 16px', borderBottom: '1px solid var(--color-border-light)', color: TEXT_DIM, fontSize: '0.78rem' }}>
            Click a node to inspect service detail — incident arrows show source (root cause) to impacted services.
          </div>
        )}

        {/* SVG Graph */}
        <div ref={containerRef} style={{ position: 'relative' }}>
          <svg
            ref={svgRef}
            width={dimensions.width}
            height={dimensions.height}
            style={{ background: BG, display: 'block' }}
          />
        </div>
      </div>

      {/* Legend */}
      <div className="card" style={{ marginTop: 'var(--spacing-md)' }}>
        <div className="card-header">
          <span className="card-title">Legend</span>
        </div>
        <div style={{ fontSize: '0.8rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 8 }}>
          <LegendItem color={DANGER} label="Critical Risk (500+)" />
          <LegendItem color={WARN} label="High Risk (200-500)" />
          <LegendItem color={MEDIUM} label="Medium Risk (50-200)" />
          <LegendItem color={DOMAIN_COLORS.infrastructure} label="Infrastructure" />
          <LegendItem color={DOMAIN_COLORS.application} label="Application" />
          <LegendItem color={DOMAIN_COLORS.network} label="Network" />
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ width: 20, height: 0, borderTop: '3px solid #58a6ff' }} />
            <span>Incident Propagation (source → impacted)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ width: 20, height: 0, borderTop: '3px solid #f85149' }} />
            <span>High Incident Count (3+)</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function MiniStat({ label, value, color }) {
  return (
    <div style={{ fontSize: '0.78rem' }}>
      <span style={{ color: TEXT_DIM }}>{label}</span><br />
      <strong style={{ color: color || '#e6edf3' }}>{value}</strong>
    </div>
  );
}

function LegendItem({ color, label }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <span style={{ width: 12, height: 12, borderRadius: '50%', background: color, flexShrink: 0 }} />
      <span>{label}</span>
    </div>
  );
}
