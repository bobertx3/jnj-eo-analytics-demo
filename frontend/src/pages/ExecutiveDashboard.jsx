import React from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  AreaChart, Area, PieChart, Pie, Cell, Legend
} from 'recharts';
import { AlertTriangle, DollarSign, Users, Clock, TrendingDown, Shield } from 'lucide-react';
import { useApi, formatNumber, formatCurrency, formatDate } from '../hooks/useApi';
import { LoadingState, ErrorState } from '../components/LoadingState';
import { SeverityBadge, DomainBadge } from '../components/SeverityBadge';
import ChartTooltip from '../components/ChartTooltip';
import InfoExpander from '../components/InfoExpander';

const SEVERITY_COLORS = {
  P1: '#f85149', P2: '#f0883e', P3: '#d29922',
};
const DOMAIN_COLORS = {
  infrastructure: '#bc8cff', application: '#58a6ff', network: '#39d353',
};

export default function ExecutiveDashboard() {
  const { data: summary, loading: summaryLoading } = useApi('/api/incidents/summary');
  const { data: timeline, loading: timelineLoading } = useApi('/api/incidents/timeline?days=90');
  const { data: domainSummary, loading: domainLoading } = useApi('/api/domains/summary');
  const { data: topIssue, loading: issueLoading } = useApi('/api/root-cause/top-systemic-issue');
  const { data: recentIncidents } = useApi('/api/incidents/recent?limit=8');
  const { data: ticketNoise } = useApi('/api/incidents/ticket-noise?days=90&limit=12');

  if (summaryLoading || timelineLoading || domainLoading) {
    return <LoadingState message="Loading executive dashboard..." />;
  }

  const s = summary || {};

  // Prepare domain pie data
  const domainPieData = (domainSummary || []).map(d => ({
    name: d.domain,
    value: Number(d.total_incidents) || 0,
    revenue: Number(d.total_revenue_impact) || 0,
  }));

  // Prepare timeline data
  const timelineData = (timeline || []).map(t => ({
    date: formatDate(t.incident_date),
    P1: Number(t.p1_count) || 0,
    P2: Number(t.p2_count) || 0,
    P3: Number(t.p3_count) || 0,
    impact: Number(t.daily_revenue_impact) || 0,
  }));

  return (
    <div>
      <div className="page-header">
        <h2>Executive Dashboard</h2>
        <p className="page-subtitle">
          Enterprise-wide root cause intelligence across 30 days of telemetry
        </p>
      </div>

      {/* Top Stat Cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Total Incidents</div>
          <div className="stat-value critical">{formatNumber(s.total_incidents)}</div>
          <div className="stat-change">
            P1: {s.p1_count} / P2: {s.p2_count} / P3: {s.p3_count}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Average MTTR</div>
          <div className="stat-value high">{s.avg_mttr || '--'}m</div>
          <div className="stat-change">Mean time to resolve</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Revenue Impact</div>
          <div className="stat-value medium">{formatCurrency(s.total_revenue_impact)}</div>
          <div className="stat-change">Cumulative financial impact</div>
          <InfoExpander title="How is this calculated?" mode="hover">
            <p style={{ marginTop: 8 }}>
              Each incident is assigned a <strong>revenue impact</strong> based on two factors:
            </p>
            <ul style={{ margin: '6px 0 0 0', paddingLeft: 18 }}>
              <li><strong>Blast radius</strong> — the number of downstream services disrupted by the incident</li>
              <li><strong>Severity</strong> — the per-service financial weight:
                <ul style={{ paddingLeft: 16, marginTop: 4 }}>
                  <li><span style={{ color: 'var(--color-critical)' }}>P1 (Critical)</span>: $5,000 – $50,000 per impacted service</li>
                  <li><span style={{ color: 'var(--color-high)' }}>P2 / P3</span>: $1,000 – $10,000 per impacted service</li>
                </ul>
              </li>
            </ul>
            <p style={{ marginTop: 8 }}>
              <strong>Formula:</strong> <code>blast_radius × severity_weight</code>
            </p>
            <p style={{ marginTop: 6 }}>
              Incidents are then bucketed into impact tiers:
            </p>
            <ul style={{ margin: '4px 0 0 0', paddingLeft: 18 }}>
              <li><strong>Critical</strong>: &gt; $100,000</li>
              <li><strong>High</strong>: $50,000 – $100,000</li>
              <li><strong>Moderate</strong>: $10,000 – $50,000</li>
              <li><strong>Low</strong>: &lt; $10,000</li>
            </ul>
            <p style={{ marginTop: 8, color: 'var(--color-text-muted)', fontStyle: 'italic' }}>
              The cumulative figure shown here sums all incidents over the selected time window.
              In production, this field would be populated from your ITSM (e.g. ServiceNow) or
              financial system of record.
            </p>
          </InfoExpander>
        </div>
        <div className="stat-card">
          <div className="stat-label">Patients Impacted</div>
          <div className="stat-value info">{formatNumber(s.total_patient_impact)}</div>
          <div className="stat-change">Across all incidents</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">SLA Breaches</div>
          <div className="stat-value critical">{formatNumber(s.total_sla_breaches)}</div>
          <div className="stat-change">Service level violations</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Avg Blast Radius</div>
          <div className="stat-value high">{s.avg_blast_radius || '--'}</div>
          <div className="stat-change">Services impacted per incident</div>
        </div>
      </div>

      {/* Top Systemic Issue Callout */}
      {topIssue && topIssue.failure_pattern_name && (
        <div className="card" style={{
          marginBottom: 'var(--spacing-xl)',
          borderLeft: '4px solid var(--color-critical)',
          background: 'rgba(248, 81, 73, 0.05)',
        }}>
          <div style={{ display: 'flex', alignItems: 'flex-start', gap: 16 }}>
            <AlertTriangle size={24} style={{ color: 'var(--color-critical)', flexShrink: 0, marginTop: 2 }} />
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--color-critical)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4 }}>
                Number 1 Systemic Issue to Fix
              </div>
              <div style={{ fontSize: '1.1rem', fontWeight: 700, marginBottom: 8 }}>
                {topIssue.failure_pattern_name}
              </div>
              <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', fontSize: '0.85rem', color: 'var(--color-text-secondary)' }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <TrendingDown size={14} /> {topIssue.occurrence_count} occurrences
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <DollarSign size={14} /> {formatCurrency(topIssue.total_revenue_impact)} impact
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <Users size={14} /> {formatNumber(topIssue.total_patient_impact)} patients
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <Clock size={14} /> Avg {topIssue.avg_mttr_minutes}m MTTR
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <Shield size={14} /> {topIssue.sla_breach_count} SLA breaches
                </span>
              </div>
              <div style={{ marginTop: 8, fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
                Root Service: <code>{topIssue.root_service}</code> | Priority Score: {topIssue.priority_score}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Charts Row */}
      <div className="grid-2-1">
        {/* Incident Timeline */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Incident Timeline (90 Days)</span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            <AreaChart data={timelineData}>
              <XAxis dataKey="date" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip />} />
              <Area type="monotone" dataKey="P1" stackId="1" fill="#f85149" stroke="#f85149" fillOpacity={0.6} />
              <Area type="monotone" dataKey="P2" stackId="1" fill="#f0883e" stroke="#f0883e" fillOpacity={0.6} />
              <Area type="monotone" dataKey="P3" stackId="1" fill="#d29922" stroke="#d29922" fillOpacity={0.6} />
              <Legend />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Domain Distribution */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Incidents by Domain</span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            <PieChart>
              <Pie
                data={domainPieData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={90}
                innerRadius={50}
                label={({ name, value }) => `${name}: ${value}`}
                labelLine={{ stroke: 'var(--color-text-muted)' }}
              >
                {domainPieData.map((entry, idx) => (
                  <Cell key={idx} fill={DOMAIN_COLORS[entry.name] || '#8b949e'} />
                ))}
              </Pie>
              <Tooltip content={<ChartTooltip formatter={(v, n) => n === 'revenue' ? formatCurrency(v) : v} />} />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Domain Risk Summary + Recent Incidents */}
      <div className="grid-2">
        {/* Domain Risk Heatmap */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Domain Impact Summary</span>
          </div>
          <InfoExpander title="How are domain risk scores and revenue calculated?">
            <p style={{ marginTop: 8 }}>
              Each row aggregates all incidents attributed to that domain (Infrastructure, Application, or Network) over the full dataset window.
            </p>
            <ul style={{ margin: '6px 0 0 0', paddingLeft: 18 }}>
              <li><strong>Revenue Impact</strong>: Sum of per-incident revenue impact for all incidents in the domain (see Revenue Impact card for the per-incident formula)</li>
              <li><strong>Patient Impact</strong>: Sum of estimated patients affected — P1/P2 incidents affect 50–500 patients; P3 affects 5–50</li>
              <li><strong>Risk Score</strong>: Composite score computed as:
                <br /><code style={{ fontSize: '0.78rem' }}>incident_count × avg_blast_radius × (1 + revenue_impact / $100K)</code>
                <br />Higher scores indicate domains with both high frequency and high financial consequence.
              </li>
            </ul>
            <p style={{ marginTop: 8, color: 'var(--color-text-muted)', fontStyle: 'italic' }}>
              Use this table to prioritize which domain deserves the next reliability investment.
            </p>
          </InfoExpander>
          <table className="data-table" style={{ marginTop: 10 }}>
            <thead>
              <tr>
                <th>Domain</th>
                <th>Incidents</th>
                <th>P1s</th>
                <th>Revenue Impact</th>
                <th>Patient Impact</th>
                <th>SLA Breaches</th>
                <th>Risk Score</th>
              </tr>
            </thead>
            <tbody>
              {(domainSummary || []).map((d) => (
                <tr key={d.domain}>
                  <td><DomainBadge domain={d.domain} /></td>
                  <td>{formatNumber(d.total_incidents)}</td>
                  <td style={{ color: Number(d.total_p1) > 0 ? 'var(--color-critical)' : 'inherit' }}>
                    {d.total_p1}
                  </td>
                  <td>{formatCurrency(d.total_revenue_impact)}</td>
                  <td>{formatNumber(d.total_patient_impact)}</td>
                  <td style={{ color: Number(d.total_sla_breaches) > 0 ? 'var(--color-critical)' : 'inherit' }}>
                    {d.total_sla_breaches}
                  </td>
                  <td style={{ fontWeight: 700 }}>{formatNumber(d.cumulative_risk_score)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Recent Incidents */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Recent Incidents</span>
          </div>
          <div style={{ maxHeight: 360, overflowY: 'auto' }}>
            {(recentIncidents || []).map((inc) => (
              <div key={inc.incident_id} style={{
                padding: '10px 0',
                borderBottom: '1px solid var(--color-border-light)',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <SeverityBadge severity={inc.severity} />
                    <span style={{ fontSize: '0.85rem', fontWeight: 500 }}>{inc.title}</span>
                  </div>
                  <span style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)' }}>
                    {formatDate(inc.created_at)}
                  </span>
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginTop: 4 }}>
                  <code>{inc.root_service}</code> | {inc.mttr_minutes}m MTTR | {inc.blast_radius} services
                  {inc.failure_pattern_name && (
                    <span> | Pattern: {inc.failure_pattern_name}</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ServiceNow Ticket Noise */}
      <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">ServiceNow Ticket Noise (90 Days)</span>
        </div>
        <InfoExpander title="What does duplicate ticket noise mean?">
          <p style={{ marginTop: 8 }}>
            This view highlights where incidents generated many duplicate ServiceNow tickets,
            a signal of alert fatigue and operational thrash.
          </p>
          <ul style={{ margin: '6px 0 0 0', paddingLeft: 18 }}>
            <li><strong>Total Tickets</strong>: all ServiceNow tickets linked to incidents for the service</li>
            <li><strong>Duplicates</strong>: repeat tickets for the same incident condition</li>
            <li><strong>Duplicate %</strong>: <code>duplicates / total_tickets</code></li>
          </ul>
        </InfoExpander>
        <table className="data-table" style={{ marginTop: 10 }}>
          <thead>
            <tr>
              <th>Service</th>
              <th>Incidents</th>
              <th>Total Tickets</th>
              <th>Duplicates</th>
              <th>Duplicate %</th>
              <th>Revenue Impact</th>
            </tr>
          </thead>
          <tbody>
            {(ticketNoise || []).map((row) => (
              <tr key={row.service_name}>
                <td><code>{row.service_name}</code></td>
                <td>{formatNumber(row.incident_count)}</td>
                <td>{formatNumber(row.total_tickets)}</td>
                <td style={{ color: Number(row.total_duplicates) > 0 ? 'var(--color-high)' : 'inherit' }}>
                  {formatNumber(row.total_duplicates)}
                </td>
                <td style={{ color: Number(row.duplicate_pct) >= 30 ? 'var(--color-critical)' : 'inherit' }}>
                  {row.duplicate_pct ?? 0}%
                </td>
                <td>{formatCurrency(row.total_revenue_impact)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
