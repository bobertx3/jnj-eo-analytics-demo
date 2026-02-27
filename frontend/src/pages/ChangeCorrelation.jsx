import React, { useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ScatterChart, Scatter, ZAxis, Cell,
  ComposedChart, Line, Area, Legend
} from 'recharts';
import { GitCompare, ArrowRight, Clock, AlertTriangle } from 'lucide-react';
import { useApi, formatNumber, formatCurrency, formatDate, formatDateTime } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';
import { SeverityBadge } from '../components/SeverityBadge';
import ChartTooltip from '../components/ChartTooltip';

const CHANGE_TYPE_COLORS = {
  deployment: '#58a6ff',
  config_change: '#f0883e',
  scaling_event: '#39d353',
  database_migration: '#f85149',
  certificate_rotation: '#bc8cff',
  firewall_rule_update: '#d29922',
  dependency_upgrade: '#ff7b72',
  feature_flag_toggle: '#79c0ff',
  infra_patch: '#7ee787',
  network_route_change: '#ffa657',
};

export default function ChangeCorrelation() {
  const { data: correlationSummary, loading: summaryLoading } = useApi('/api/changes/correlation-summary');
  const { data: riskyTypes, loading: riskyLoading } = useApi('/api/changes/risky-change-types');
  const { data: highCorrelation } = useApi('/api/changes/high-correlation?min_strength=0.3');
  const { data: byExecutor } = useApi('/api/changes/by-executor');
  const [timelineDays, setTimelineDays] = useState(30);
  const { data: timelineData } = useApi(`/api/changes/timeline?days=${timelineDays}`);

  if (summaryLoading || riskyLoading) return <LoadingState message="Loading change correlation data..." />;

  // Risky change types bar chart
  const riskyBarData = (riskyTypes || []).map(r => ({
    name: r.change_type?.replace(/_/g, ' '),
    rate: Number(r.incident_rate_pct) || 0,
    incidents: Number(r.incidents_caused) || 0,
    total: Number(r.total_changes) || 0,
  })).sort((a, b) => b.rate - a.rate);

  // Correlation strength by type
  const corrData = (correlationSummary || []).map(c => ({
    name: c.change_type?.replace(/_/g, ' '),
    strength: Number(c.avg_correlation_strength) || 0,
    maxStrength: Number(c.max_correlation_strength) || 0,
    avgTime: Number(c.avg_time_to_incident_min) || 0,
    impact: Number(c.total_revenue_impact) || 0,
  })).sort((a, b) => b.strength - a.strength);

  // Timeline overlay data
  const changes = timelineData?.changes || [];
  const incidents = timelineData?.incidents || [];

  // Group by date for overlay chart
  const dateMap = {};
  changes.forEach(c => {
    const d = formatDate(c.executed_at);
    if (!dateMap[d]) dateMap[d] = { date: d, changes: 0, incidents: 0, highRisk: 0 };
    dateMap[d].changes++;
    if (c.risk_level === 'high') dateMap[d].highRisk++;
  });
  incidents.forEach(i => {
    const d = formatDate(i.created_at);
    if (!dateMap[d]) dateMap[d] = { date: d, changes: 0, incidents: 0, highRisk: 0 };
    dateMap[d].incidents++;
  });
  const overlayData = Object.values(dateMap).sort((a, b) => a.date.localeCompare(b.date));

  return (
    <div>
      <div className="page-header">
        <h2>Change Correlation Analysis</h2>
        <p className="page-subtitle">
          Statistical correlation between topology changes and incidents
        </p>
      </div>

      {/* Stat Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Total Changes</div>
          <div className="stat-value info">
            {formatNumber((riskyTypes || []).reduce((s, r) => s + Number(r.total_changes || 0), 0))}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Incident-Causing Changes</div>
          <div className="stat-value critical">
            {formatNumber((riskyTypes || []).reduce((s, r) => s + Number(r.incidents_caused || 0), 0))}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Riskiest Change Type</div>
          <div className="stat-value high" style={{ fontSize: '1.1rem' }}>
            {riskyBarData[0]?.name || '--'}
          </div>
          <div className="stat-change">{riskyBarData[0]?.rate}% incident rate</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Highest Correlation</div>
          <div className="stat-value medium">
            {corrData[0]?.strength.toFixed(2) || '--'}
          </div>
          <div className="stat-change">{corrData[0]?.name}</div>
        </div>
      </div>

      {/* Change vs Incident Timeline */}
      <div className="card" style={{ marginBottom: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Changes vs Incidents Timeline</span>
          <select
            className="filter-select"
            value={timelineDays}
            onChange={(e) => setTimelineDays(Number(e.target.value))}
          >
            <option value={30}>30 Days</option>
            <option value={60}>60 Days</option>
            <option value={90}>90 Days</option>
          </select>
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <ComposedChart data={overlayData}>
            <XAxis dataKey="date" tick={{ fontSize: 9 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip content={<ChartTooltip />} />
            <Area type="monotone" dataKey="changes" fill="var(--chart-1)" stroke="var(--chart-1)" fillOpacity={0.2} name="Changes" />
            <Bar dataKey="incidents" fill="var(--chart-5)" name="Incidents" barSize={6} />
            <Line type="monotone" dataKey="highRisk" stroke="var(--chart-2)" name="High-Risk Changes" dot={false} strokeWidth={2} />
            <Legend />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className="grid-2">
        {/* Incident Rate by Change Type */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Incident Rate by Change Type</span>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={riskyBarData} layout="vertical" margin={{ left: 20 }}>
              <XAxis type="number" tick={{ fontSize: 10 }} unit="%" />
              <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip formatter={(v, n) => n === 'rate' ? `${v}%` : v} />} />
              <Bar dataKey="rate" name="Incident Rate %" radius={[0, 4, 4, 0]}>
                {riskyBarData.map((d, i) => (
                  <Cell key={i} fill={d.rate > 10 ? '#f85149' : d.rate > 5 ? '#f0883e' : '#58a6ff'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Correlation Strength */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Avg Correlation Strength by Type</span>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={corrData} layout="vertical" margin={{ left: 20 }}>
              <XAxis type="number" tick={{ fontSize: 10 }} domain={[0, 1]} />
              <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="strength" name="Avg Strength" fill="var(--chart-3)" radius={[0, 4, 4, 0]} />
              <Bar dataKey="maxStrength" name="Max Strength" fill="var(--chart-2)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* High-Correlation Pairs Table */}
      <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">High-Correlation Change-Incident Pairs</span>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Correlation</th>
                <th>Change</th>
                <th>Type</th>
                <th>Risk</th>
                <th></th>
                <th>Incident</th>
                <th>Severity</th>
                <th>Time Gap</th>
                <th>Window</th>
                <th>Revenue Impact</th>
              </tr>
            </thead>
            <tbody>
              {(highCorrelation || []).slice(0, 20).map((pair) => (
                <tr key={`${pair.change_id}-${pair.incident_id}`}>
                  <td>
                    <span style={{
                      fontWeight: 700,
                      color: Number(pair.correlation_strength) > 0.7 ? 'var(--color-critical)' :
                             Number(pair.correlation_strength) > 0.4 ? 'var(--color-high)' : 'var(--color-medium)',
                    }}>
                      {Number(pair.correlation_strength).toFixed(3)}
                    </span>
                  </td>
                  <td className="mono">{pair.change_service}</td>
                  <td>
                    <span className="badge badge-info" style={{ fontSize: '0.65rem' }}>
                      {pair.change_type?.replace(/_/g, ' ')}
                    </span>
                  </td>
                  <td>
                    <span className={`badge badge-${pair.risk_level === 'high' ? 'critical' : pair.risk_level === 'medium' ? 'medium' : 'low'}`}>
                      {pair.risk_level}
                    </span>
                  </td>
                  <td style={{ color: 'var(--color-text-muted)' }}>
                    <ArrowRight size={14} />
                  </td>
                  <td style={{ fontSize: '0.8rem' }}>{pair.incident_title}</td>
                  <td><SeverityBadge severity={pair.incident_severity} /></td>
                  <td>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: '0.8rem' }}>
                      <Clock size={12} />
                      {Number(pair.minutes_between).toFixed(0)}m
                    </span>
                  </td>
                  <td>
                    <span className={`badge badge-${pair.correlation_window === 'immediate' ? 'critical' : pair.correlation_window === 'short_delay' ? 'high' : 'medium'}`}>
                      {pair.correlation_window}
                    </span>
                  </td>
                  <td>{formatCurrency(pair.revenue_impact_usd)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* By Executor */}
      <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Changes by Executor</span>
        </div>
        <table className="data-table">
          <thead>
            <tr>
              <th>Executor</th>
              <th>Total Changes</th>
              <th>Incidents (4h)</th>
              <th>Incidents (24h)</th>
              <th>High Risk</th>
              <th>Avg Risk Score</th>
            </tr>
          </thead>
          <tbody>
            {(byExecutor || []).map(e => (
              <tr key={e.executed_by}>
                <td><code>{e.executed_by}</code></td>
                <td>{e.total_changes}</td>
                <td style={{ color: Number(e.incidents_caused_4h) > 0 ? 'var(--color-critical)' : 'inherit' }}>
                  {e.incidents_caused_4h}
                </td>
                <td>{e.incidents_caused_24h}</td>
                <td style={{ color: Number(e.high_risk_changes) > 0 ? 'var(--color-high)' : 'inherit' }}>
                  {e.high_risk_changes}
                </td>
                <td>{Number(e.avg_risk_score).toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
