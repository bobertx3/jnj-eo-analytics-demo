import React, { useMemo, useState } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, Cell, Legend,
  LineChart, Line
} from 'recharts';
import { Layers, Server, AppWindow, Wifi, ChevronDown } from 'lucide-react';
import { useApi, formatNumber, formatCurrency, formatDate, formatDateTime } from '../hooks/useApi';
import { LoadingState } from '../components/LoadingState';
import { SeverityBadge, DomainBadge } from '../components/SeverityBadge';
import ChartTooltip from '../components/ChartTooltip';
import InfoExpander from '../components/InfoExpander';

const DOMAINS = [
  { key: 'infrastructure', label: 'Infrastructure', icon: Server, color: '#bc8cff' },
  { key: 'application', label: 'Application', icon: AppWindow, color: '#58a6ff' },
  { key: 'network', label: 'Network', icon: Wifi, color: '#39d353' },
];

export default function DomainDeepDive() {
  const [selectedDomain, setSelectedDomain] = useState('infrastructure');
  const [selectedIncidentId, setSelectedIncidentId] = useState(null);
  const { data: domainSummary } = useApi('/api/domains/summary');
  const { data: domainTrend } = useApi(`/api/domains/trend?domain=${selectedDomain}&days=90`);
  const { data: domainServices } = useApi(`/api/domains/${selectedDomain}/services`);
  const { data: domainIncidents } = useApi(`/api/domains/${selectedDomain}/incidents?days=90&limit=30`);
  const { data: domainAlerts } = useApi(`/api/domains/${selectedDomain}/alerts?days=30`);
  const { data: incidentDetail, loading: incidentDetailLoading } = useApi(
    selectedIncidentId ? `/api/incidents/${selectedIncidentId}` : null
  );

  const currentDomain = (domainSummary || []).find(d => d.domain === selectedDomain) || {};
  const domainConfig = DOMAINS.find(d => d.key === selectedDomain);
  const impactedServices = useMemo(() => {
    const raw = incidentDetail?.impacted_services;
    if (!raw) return [];
    if (Array.isArray(raw)) return raw;
    if (typeof raw === 'string') {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed;
      } catch {
        // Continue with lightweight normalization fallback.
      }
      return raw
        .replace(/^\[|\]$/g, '')
        .split(',')
        .map((s) => s.replace(/^['"\s]+|['"\s]+$/g, ''))
        .filter(Boolean);
    }
    return [];
  }, [incidentDetail]);

  // Trend data for charts
  const trendData = (domainTrend || []).map(t => ({
    week: formatDate(t.week_start),
    incidents: Number(t.weekly_incidents) || 0,
    p1: Number(t.weekly_p1) || 0,
    revenue: Number(t.weekly_revenue_impact) || 0,
    risk: Number(t.avg_risk_score) || 0,
    changes: Number(t.weekly_changes) || 0,
    alerts: Number(t.weekly_alerts) || 0,
  }));

  // Service bar data
  const serviceBarData = (domainServices || []).map(s => ({
    name: s.service_name,
    risk: Number(s.risk_score) || 0,
    incidents: Number(s.incident_count_as_root) || 0,
    health: Number(s.avg_health_score) || 0,
  }));

  return (
    <div>
      <div className="page-header">
        <h2>Domain Deep Dive</h2>
        <p className="page-subtitle">
          Per-domain incident and alert explorer across Infrastructure, Application, and Network
        </p>
      </div>

      {/* Domain Selector */}
      <div style={{
        display: 'flex', gap: 12, marginBottom: 'var(--spacing-xl)',
      }}>
        {DOMAINS.map(d => {
          const Icon = d.icon;
          const isActive = selectedDomain === d.key;
          const summary = (domainSummary || []).find(ds => ds.domain === d.key) || {};
          return (
            <button
              key={d.key}
              onClick={() => setSelectedDomain(d.key)}
              style={{
                flex: 1,
                display: 'flex',
                alignItems: 'center',
                gap: 12,
                padding: '16px 20px',
                background: isActive ? 'var(--color-bg-card)' : 'transparent',
                border: `2px solid ${isActive ? d.color : 'var(--color-border)'}`,
                borderRadius: 'var(--radius-lg)',
                cursor: 'pointer',
                transition: 'all 0.15s',
                color: 'var(--color-text-primary)',
              }}
            >
              <Icon size={24} style={{ color: d.color }} />
              <div style={{ textAlign: 'left' }}>
                <div style={{ fontWeight: 700, fontSize: '0.95rem' }}>{d.label}</div>
                <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)' }}>
                  {formatNumber(summary.total_incidents)} incidents | {formatCurrency(summary.total_revenue_impact)}
                </div>
              </div>
            </button>
          );
        })}
      </div>

      {/* Domain Stats */}
      <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(5, 1fr)' }}>
        <div className="stat-card">
          <div className="stat-label">Incidents</div>
          <div className="stat-value" style={{ color: domainConfig?.color }}>
            {formatNumber(currentDomain.total_incidents)}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">P1 Incidents</div>
          <div className="stat-value critical">{formatNumber(currentDomain.total_p1)}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Avg MTTR</div>
          <div className="stat-value high">{currentDomain.avg_mttr || '--'}m</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Revenue Impact</div>
          <div className="stat-value medium">{formatCurrency(currentDomain.total_revenue_impact)}</div>
          <div className="stat-change">Cumulative domain financial impact</div>
          <InfoExpander title="How is this calculated?" mode="hover">
            <p style={{ marginTop: 8 }}>
              Domain revenue impact is the <strong>sum of all incident-level revenue impact</strong> for
              the selected domain over the current analysis window.
            </p>
            <ul style={{ margin: '6px 0 0 0', paddingLeft: 18 }}>
              <li>Each incident contributes based on <strong>blast radius</strong> and <strong>severity</strong></li>
              <li><span style={{ color: 'var(--color-critical)' }}>P1 (Critical)</span>: higher per-service cost band</li>
              <li><span style={{ color: 'var(--color-high)' }}>P2 / P3</span>: lower per-service cost band</li>
            </ul>
            <p style={{ marginTop: 8 }}>
              <strong>Per-incident formula:</strong> <code>blast_radius × severity_weight</code>
            </p>
            <p style={{ marginTop: 6, color: 'var(--color-text-muted)', fontStyle: 'italic' }}>
              This is an estimated operational impact metric used for prioritization, not a booked-finance number.
            </p>
          </InfoExpander>
        </div>
        <div className="stat-card">
          <div className="stat-label">SLA Breaches</div>
          <div className="stat-value critical">{formatNumber(currentDomain.total_sla_breaches)}</div>
        </div>
      </div>

      {/* Trend Charts */}
      <div className="grid-2">
        <div className="card">
          <div className="card-header">
            <span className="card-title">Weekly Incident Trend</span>
          </div>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={trendData}>
              <XAxis dataKey="week" tick={{ fontSize: 9 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip />} />
              <Area type="monotone" dataKey="incidents" fill={domainConfig?.color || '#58a6ff'} stroke={domainConfig?.color} fillOpacity={0.3} name="Incidents" />
              <Area type="monotone" dataKey="p1" fill="#f85149" stroke="#f85149" fillOpacity={0.3} name="P1" />
              <Legend />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <div className="card">
          <div className="card-header">
            <span className="card-title">Weekly Revenue Impact & Risk</span>
          </div>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={trendData}>
              <XAxis dataKey="week" tick={{ fontSize: 9 }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip formatter={(v, n) => n.includes('Revenue') ? formatCurrency(v) : v} />} />
              <Line type="monotone" dataKey="revenue" stroke="var(--chart-2)" name="Revenue Impact" dot={false} />
              <Line type="monotone" dataKey="risk" stroke="var(--chart-5)" name="Risk Score" dot={false} />
              <Legend />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        {/* Services in Domain */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Services in {domainConfig?.label}</span>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={serviceBarData} layout="vertical" margin={{ left: 10 }}>
              <XAxis type="number" tick={{ fontSize: 10 }} />
              <YAxis type="category" dataKey="name" width={160} tick={{ fontSize: 10 }} />
              <Tooltip content={<ChartTooltip />} />
              <Bar dataKey="risk" name="Risk Score" fill={domainConfig?.color || '#58a6ff'} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <div style={{ marginTop: 12, overflowX: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Incidents</th>
                  <th>P1</th>
                  <th>Revenue</th>
                  <th>Health</th>
                </tr>
              </thead>
              <tbody>
                {(domainServices || []).map(s => (
                  <tr key={s.service_name}>
                    <td><code>{s.service_name}</code></td>
                    <td>{s.incident_count_as_root}</td>
                    <td style={{ color: Number(s.p1_count) > 0 ? 'var(--color-critical)' : 'inherit' }}>{s.p1_count}</td>
                    <td>{formatCurrency(s.total_revenue_impact)}</td>
                    <td style={{
                      color: Number(s.avg_health_score) < 50 ? 'var(--color-critical)' :
                             Number(s.avg_health_score) < 75 ? 'var(--color-medium)' : 'var(--color-low)',
                    }}>{Number(s.avg_health_score).toFixed(0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Alerts Summary */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Alert Summary (30 Days)</span>
          </div>
          <div style={{ maxHeight: 500, overflowY: 'auto' }}>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Alert</th>
                  <th>Count</th>
                  <th>Critical</th>
                  <th>Incident Link</th>
                  <th>Pre-Inc</th>
                </tr>
              </thead>
              <tbody>
                {(domainAlerts || []).slice(0, 30).map((a, idx) => (
                  <tr key={idx}>
                    <td><code style={{ fontSize: '0.75rem' }}>{a.service}</code></td>
                    <td style={{ fontSize: '0.8rem' }}>{a.alert_name}</td>
                    <td>{a.alert_count}</td>
                    <td style={{ color: Number(a.critical_count) > 0 ? 'var(--color-critical)' : 'inherit' }}>
                      {a.critical_count}
                    </td>
                    <td>{a.incident_correlated}</td>
                    <td style={{ color: Number(a.pre_incident_count) > 0 ? 'var(--color-high)' : 'inherit' }}>
                      {a.pre_incident_count}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Recent Domain Incidents */}
      <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Recent {domainConfig?.label} Incidents</span>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Severity</th>
                <th>Title</th>
                <th>Root Service</th>
                <th>Created</th>
                <th>MTTR</th>
                <th>Blast</th>
                <th>Pattern</th>
                <th>Revenue</th>
                <th>Patients</th>
                <th>SLA</th>
              </tr>
            </thead>
            <tbody>
              {(domainIncidents || []).map(inc => (
                <tr key={inc.incident_id}>
                  <td className="mono">
                    <button
                      className="btn btn-secondary btn-sm"
                      style={{ padding: '2px 8px' }}
                      onClick={() => setSelectedIncidentId(inc.incident_id)}
                    >
                      {inc.incident_id}
                    </button>
                  </td>
                  <td><SeverityBadge severity={inc.severity} /></td>
                  <td>{inc.title}</td>
                  <td><code>{inc.root_service}</code></td>
                  <td>{formatDateTime(inc.created_at)}</td>
                  <td>{inc.mttr_minutes}m</td>
                  <td>{inc.blast_radius}</td>
                  <td style={{ fontSize: '0.75rem', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {inc.failure_pattern_name || '--'}
                  </td>
                  <td>{formatCurrency(inc.revenue_impact_usd)}</td>
                  <td>{inc.patient_impact_count}</td>
                  <td style={{ color: inc.sla_breached === 'true' ? 'var(--color-critical)' : 'inherit' }}>
                    {inc.sla_breached === 'true' ? 'YES' : 'No'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {selectedIncidentId && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(1, 4, 9, 0.28)',
            zIndex: 1000,
            display: 'flex',
            justifyContent: 'flex-end',
            alignItems: 'stretch',
            padding: 0,
          }}
          onClick={() => setSelectedIncidentId(null)}
        >
          <div
            className="card"
            style={{
              width: 'min(760px, 92vw)',
              height: '100vh',
              overflowY: 'auto',
              borderRadius: 0,
              border: '1px solid var(--color-border)',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span className="card-title">Incident Payload: {selectedIncidentId}</span>
              <button className="btn btn-secondary btn-sm" onClick={() => setSelectedIncidentId(null)}>
                Close
              </button>
            </div>

            {incidentDetailLoading ? (
              <LoadingState message="Loading incident detail..." />
            ) : !incidentDetail || !incidentDetail.incident_id ? (
              <div style={{ padding: 16, color: 'var(--color-text-muted)' }}>No incident detail found.</div>
            ) : (
              <div style={{ padding: 16 }}>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0,1fr))', gap: 12, marginBottom: 16 }}>
                  <div><div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)' }}>Severity</div><div><SeverityBadge severity={incidentDetail.severity} /></div></div>
                  <div><div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)' }}>Root Service</div><div><code>{incidentDetail.root_service}</code></div></div>
                  <div><div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)' }}>Created</div><div>{formatDateTime(incidentDetail.created_at)}</div></div>
                  <div><div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)' }}>MTTR</div><div>{incidentDetail.mttr_minutes}m</div></div>
                </div>

                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Title</div>
                  <div style={{ fontWeight: 700 }}>{incidentDetail.title}</div>
                </div>
                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Description</div>
                  <div>{incidentDetail.description || '--'}</div>
                </div>
                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Root Cause Explanation</div>
                  <div>{incidentDetail.root_cause_explanation || '--'}</div>
                </div>
                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Impacted Services</div>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {impactedServices.map((svc) => (
                      <span key={svc} className="badge badge-info">{svc}</span>
                    ))}
                  </div>
                </div>

                <div style={{ marginTop: 16 }}>
                  <div style={{ fontSize: '0.72rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', marginBottom: 8 }}>
                    Raw Incident JSON
                  </div>
                  <pre style={{
                    background: 'var(--color-bg-elevated)',
                    border: '1px solid var(--color-border)',
                    borderRadius: 'var(--radius-sm)',
                    padding: 12,
                    overflowX: 'auto',
                    fontSize: '0.75rem',
                    lineHeight: 1.4,
                  }}>
                    {JSON.stringify(incidentDetail, null, 2)}
                  </pre>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
