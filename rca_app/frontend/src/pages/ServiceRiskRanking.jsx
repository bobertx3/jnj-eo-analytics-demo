import React, { useEffect, useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ScatterChart, Scatter, ZAxis, Cell,
  LineChart, Line, Legend
} from 'recharts';
import { Shield, AlertCircle, ArrowUpRight } from 'lucide-react';
import { useApi, formatNumber, formatCurrency, formatDate } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';
import { SeverityBadge } from '../components/SeverityBadge';
import ChartTooltip from '../components/ChartTooltip';

const RISK_COLORS = [
  '#f85149', '#f85149', '#f0883e', '#f0883e', '#d29922',
  '#d29922', '#58a6ff', '#58a6ff', '#39d353', '#39d353',
];

export default function ServiceRiskRanking() {
  const { data: ranking, loading } = useApi('/api/services/risk-ranking');
  const [selectedService, setSelectedService] = useState(null);
  const services = ranking || [];

  // Keep detail panel populated by default and resilient to data refreshes.
  useEffect(() => {
    if (services.length === 0) {
      if (selectedService) setSelectedService(null);
      return;
    }
    const hasSelected = selectedService && services.some((s) => s.service_name === selectedService);
    if (!hasSelected) {
      setSelectedService(services[0].service_name);
    }
  }, [services, selectedService]);

  const { data: healthTimeline } = useApi(
    selectedService ? `/api/services/health-timeline?service=${selectedService}&days=90` : null
  );
  const { data: serviceIncidents } = useApi(
    selectedService ? `/api/services/${selectedService}/incidents?limit=10` : null
  );

  if (loading) return <LoadingState message="Loading service risk rankings..." />;

  // Bar chart data
  const barData = services.slice(0, 15).map((s, i) => ({
    name: s.service_name,
    risk: Number(s.risk_score) || 0,
    incidents: Number(s.incident_count_as_root) || 0,
  }));

  // Scatter: incidents vs revenue impact (bubble = blast radius)
  const scatterData = services.filter(s => Number(s.incident_count_as_root) > 0).map(s => ({
    name: s.service_name,
    x: Number(s.incident_count_as_root) || 0,
    y: Number(s.total_revenue_impact) || 0,
    z: Number(s.avg_blast_radius) || 1,
    risk: Number(s.risk_score) || 0,
  }));

  return (
    <div>
      <div className="page-header">
        <h2>Service Risk Ranking</h2>
        <p className="page-subtitle">
          Services ranked by incident frequency, blast radius, and business impact
        </p>
      </div>

      {/* Top Risk Bar Chart */}
      <div className="card" style={{ marginBottom: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Risk Score by Service</span>
        </div>
        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={barData} margin={{ left: 10 }}>
            <XAxis dataKey="name" tick={{ fontSize: 9 }} angle={-35} textAnchor="end" height={80} />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip content={<ChartTooltip />} />
            <Bar dataKey="risk" name="Risk Score" radius={[4, 4, 0, 0]}>
              {barData.map((_, i) => (
                <Cell key={i} fill={RISK_COLORS[Math.min(i, RISK_COLORS.length - 1)]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="grid-2">
        {/* Scatter Plot: Incidents vs Revenue */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Incidents vs Revenue Impact</span>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart margin={{ bottom: 5 }}>
              <XAxis dataKey="x" name="Incidents" tick={{ fontSize: 10 }} label={{ value: 'Incidents', position: 'bottom', fontSize: 10 }} />
              <YAxis dataKey="y" name="Revenue Impact" tick={{ fontSize: 10 }} tickFormatter={(v) => `$${(v/1000).toFixed(0)}K`} />
              <ZAxis dataKey="z" range={[100, 800]} name="Blast Radius" />
              <Tooltip content={({ payload }) => {
                if (!payload?.[0]) return null;
                const d = payload[0].payload;
                return (
                  <div style={{
                    background: 'var(--color-bg-elevated)', border: '1px solid var(--color-border)',
                    borderRadius: 8, padding: '10px 14px', fontSize: '0.8rem',
                  }}>
                    <div style={{ fontWeight: 700, marginBottom: 4 }}>{d.name}</div>
                    <div>Incidents: {d.x}</div>
                    <div>Revenue: {formatCurrency(d.y)}</div>
                    <div>Avg Blast: {d.z}</div>
                  </div>
                );
              }} />
              <Scatter data={scatterData} fill="var(--color-accent)">
                {scatterData.map((s, i) => (
                  <Cell key={i} fill={s.risk > 500 ? '#f85149' : s.risk > 200 ? '#f0883e' : '#58a6ff'} />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* Service Detail Panel */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Service Detail</span>
            <select
              className="filter-select"
              value={selectedService || ''}
              onChange={(e) => setSelectedService(e.target.value || null)}
            >
              <option value="">Select a service...</option>
              {services.map(s => (
                <option key={s.service_name} value={s.service_name}>{s.service_name}</option>
              ))}
            </select>
          </div>
          {selectedService && healthTimeline ? (
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={(healthTimeline || []).map(h => ({
                date: formatDate(h.health_date),
                health: Number(h.health_score) || 0,
                cpu: Number(h.avg_cpu_pct) || 0,
                errors: Number(h.error_rate_pct) || 0,
              }))}>
                <XAxis dataKey="date" tick={{ fontSize: 9 }} interval="preserveStartEnd" />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip content={<ChartTooltip />} />
                <Line type="monotone" dataKey="health" stroke="var(--chart-4)" name="Health Score" dot={false} />
                <Line type="monotone" dataKey="cpu" stroke="var(--chart-1)" name="CPU %" dot={false} />
                <Line type="monotone" dataKey="errors" stroke="var(--chart-5)" name="Error %" dot={false} />
                <Legend />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ textAlign: 'center', padding: 40, color: 'var(--color-text-muted)' }}>
              Select a service to view health timeline
            </div>
          )}
        </div>
      </div>

      {/* Full Ranking Table */}
      <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Complete Service Risk Ranking</span>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Service</th>
                <th>Risk Score</th>
                <th>Incidents (Root)</th>
                <th>Times Impacted</th>
                <th>P1</th>
                <th>Avg MTTR</th>
                <th>Revenue Impact</th>
                <th>User Impact</th>
                <th>Health Score</th>
                <th>Error Rate</th>
                <th>Risky Changes</th>
              </tr>
            </thead>
            <tbody>
              {services.map((s) => {
                const risk = Number(s.risk_score) || 0;
                return (
                  <tr
                    key={s.service_name}
                    onClick={() => setSelectedService(s.service_name)}
                    style={{ cursor: 'pointer' }}
                  >
                    <td>
                      <span style={{
                        fontWeight: 700,
                        color: risk > 500 ? 'var(--color-critical)' :
                               risk > 200 ? 'var(--color-high)' : 'var(--color-text-secondary)',
                      }}>
                        #{s.risk_rank}
                      </span>
                    </td>
                    <td>
                      <code style={{ fontSize: '0.8rem' }}>{s.service_name}</code>
                    </td>
                    <td style={{
                      fontWeight: 700,
                      color: risk > 500 ? 'var(--color-critical)' :
                             risk > 200 ? 'var(--color-high)' :
                             risk > 50 ? 'var(--color-medium)' : 'var(--color-low)',
                    }}>
                      {risk.toFixed(0)}
                    </td>
                    <td>{s.incident_count_as_root}</td>
                    <td>{s.times_impacted_by_others}</td>
                    <td style={{ color: Number(s.p1_count) > 0 ? 'var(--color-critical)' : 'inherit' }}>
                      {s.p1_count}
                    </td>
                    <td>{Number(s.avg_mttr_minutes).toFixed(0)}m</td>
                    <td>{formatCurrency(s.total_revenue_impact)}</td>
                    <td>{formatNumber(s.total_user_impact)}</td>
                    <td style={{
                      color: Number(s.avg_health_score) < 50 ? 'var(--color-critical)' :
                             Number(s.avg_health_score) < 75 ? 'var(--color-medium)' : 'var(--color-low)',
                    }}>
                      {Number(s.avg_health_score).toFixed(0)}
                    </td>
                    <td>{Number(s.avg_error_rate).toFixed(1)}%</td>
                    <td style={{ color: Number(s.risky_changes) > 0 ? 'var(--color-high)' : 'inherit' }}>
                      {s.risky_changes}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
