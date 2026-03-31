import React, { useState, useEffect } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar,
  Legend
} from 'recharts';
import { Brain, Sparkles, AlertTriangle, TrendingUp, RefreshCw, Mail, MailX } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import { useApi, useApiPost, formatNumber, formatCurrency } from '../hooks/useApi';
import { LoadingState, ErrorState } from '../components/LoadingState';
import { SeverityBadge, TrendBadge, DomainBadge } from '../components/SeverityBadge';
import ChartTooltip from '../components/ChartTooltip';

export default function RootCauseIntelligence() {
  const { data: patterns, loading } = useApi('/api/root-cause/patterns');
  const { data: aiResult, loading: aiLoading, postData: requestAI } = useApiPost();
  const [selectedPattern, setSelectedPattern] = useState(null);
  const patternList = patterns || [];

  const handleAIAnalysis = async (patternId) => {
    const endpoint = patternId
      ? `/api/root-cause/ai-analysis?pattern_id=${patternId}`
      : '/api/root-cause/ai-analysis';
    await requestAI(endpoint);
  };

  // Keep detail panel in sync with fetched patterns.
  useEffect(() => {
    if (patternList.length === 0) {
      if (selectedPattern) setSelectedPattern(null);
      return;
    }
    const selectedId = selectedPattern?.failure_pattern_id;
    const hasSelected = selectedId && patternList.some((p) => p.failure_pattern_id === selectedId);
    if (!hasSelected) {
      setSelectedPattern(patternList[0]);
    }
  }, [patternList, selectedPattern]);

  if (loading) return <LoadingState message="Loading root cause patterns..." />;

  // Prepare bar chart data (top patterns by priority)
  const chartData = patternList.slice(0, 8).map(p => ({
    name: (p.failure_pattern_name || '').replace(/\s+/g, ' ').substring(0, 30),
    score: Number(p.priority_score) || 0,
    occurrences: Number(p.occurrence_count) || 0,
    revenue: Number(p.total_revenue_impact) || 0,
  }));

  // Radar data for top pattern
  const topPattern = patternList[0] || {};
  const maxValues = {
    occurrences: Math.max(...patternList.map(p => Number(p.occurrence_count) || 0), 1),
    mttr: Math.max(...patternList.map(p => Number(p.avg_mttr_minutes) || 0), 1),
    blast: Math.max(...patternList.map(p => Number(p.avg_blast_radius) || 0), 1),
    revenue: Math.max(...patternList.map(p => Number(p.total_revenue_impact) || 0), 1),
    user: Math.max(...patternList.map(p => Number(p.total_user_impact) || 0), 1),
    sla: Math.max(...patternList.map(p => Number(p.sla_breach_count) || 0), 1),
  };

  const radarData = selectedPattern ? [
    { metric: 'Frequency', value: ((Number(selectedPattern.occurrence_count) || 0) / maxValues.occurrences) * 100 },
    { metric: 'MTTR', value: ((Number(selectedPattern.avg_mttr_minutes) || 0) / maxValues.mttr) * 100 },
    { metric: 'Blast Radius', value: ((Number(selectedPattern.avg_blast_radius) || 0) / maxValues.blast) * 100 },
    { metric: 'Revenue', value: ((Number(selectedPattern.total_revenue_impact) || 0) / maxValues.revenue) * 100 },
    { metric: 'User Impact', value: ((Number(selectedPattern.total_user_impact) || 0) / maxValues.user) * 100 },
    { metric: 'SLA Breaches', value: ((Number(selectedPattern.sla_breach_count) || 0) / maxValues.sla) * 100 },
  ] : [];

  return (
    <div>
      <div className="page-header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h2>Root Cause Intelligence</h2>
            <p className="page-subtitle">
              AI-powered pattern detection revealing systemic failure signatures
            </p>
          </div>
          <button
            className="btn btn-primary"
            onClick={() => handleAIAnalysis(selectedPattern?.failure_pattern_id)}
            disabled={aiLoading}
          >
            {aiLoading ? <RefreshCw size={14} className="spinner-inline" /> : <Brain size={14} />}
            {aiLoading ? 'Analyzing...' : 'Generate AI Analysis'}
          </button>
        </div>
      </div>

      {/* Priority Score Chart */}
      <div className="card" style={{ marginBottom: 'var(--spacing-lg)' }}>
        <div className="card-header">
          <span className="card-title">Failure Patterns by Priority Score</span>
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData} layout="vertical" margin={{ left: 20 }}>
            <XAxis type="number" tick={{ fontSize: 10 }} />
            <YAxis type="category" dataKey="name" width={220} tick={{ fontSize: 10 }} />
            <Tooltip content={<ChartTooltip formatter={(v, n) => n === 'revenue' ? formatCurrency(v) : formatNumber(v)} />} />
            <Bar dataKey="score" fill="var(--chart-1)" radius={[0, 4, 4, 0]} name="Priority Score" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="grid-2-1">
        {/* Pattern List */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Recurring Failure Patterns ({patternList.length})</span>
          </div>
          <div style={{ maxHeight: 600, overflowY: 'auto' }}>
            {patternList.map((p, idx) => (
              <div
                key={p.failure_pattern_id}
                onClick={() => setSelectedPattern(p)}
                style={{
                  padding: '14px',
                  borderBottom: '1px solid var(--color-border-light)',
                  cursor: 'pointer',
                  background: selectedPattern?.failure_pattern_id === p.failure_pattern_id
                    ? 'rgba(31, 111, 235, 0.08)' : 'transparent',
                  borderLeft: selectedPattern?.failure_pattern_id === p.failure_pattern_id
                    ? '3px solid var(--color-accent)' : '3px solid transparent',
                  transition: 'all 0.15s',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                      <span style={{
                        fontSize: '0.7rem', fontWeight: 700,
                        color: 'var(--color-text-muted)',
                        minWidth: 24,
                      }}>#{idx + 1}</span>
                      <span style={{ fontWeight: 600, fontSize: '0.9rem' }}>
                        {p.failure_pattern_name}
                      </span>
                    </div>
                    <div style={{ display: 'flex', gap: 12, fontSize: '0.75rem', color: 'var(--color-text-muted)', flexWrap: 'wrap' }}>
                      <DomainBadge domain={p.domain} />
                      <TrendBadge trend={p.trend_direction} />
                      <span>{p.occurrence_count}x occurrences</span>
                      <span>{formatCurrency(p.total_revenue_impact)}</span>
                      <span>{formatNumber(p.total_user_impact)} users</span>
                    </div>
                  </div>
                  <div style={{
                    textAlign: 'right', minWidth: 80,
                  }}>
                    <div style={{ fontSize: '1.1rem', fontWeight: 800, color: 'var(--color-accent)' }}>
                      {Number(p.priority_score).toFixed(0)}
                    </div>
                    <div style={{ fontSize: '0.65rem', color: 'var(--color-text-muted)' }}>PRIORITY</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Pattern Detail / Radar */}
        <div>
          {selectedPattern ? (
            <div className="card">
              <div className="card-header">
                <span className="card-title">Pattern Analysis</span>
              </div>
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontWeight: 700, fontSize: '1rem', marginBottom: 8 }}>
                  {selectedPattern.failure_pattern_name}
                </div>
                <div style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)', marginBottom: 12 }}>
                  Root Service: <code>{selectedPattern.root_service}</code>
                </div>
                {selectedPattern.root_cause_explanation && (
                  <div style={{
                    fontSize: '0.82rem',
                    lineHeight: 1.5,
                    marginBottom: 14,
                    padding: '10px 12px',
                    background: 'rgba(248, 81, 73, 0.06)',
                    borderLeft: '3px solid var(--color-critical)',
                    borderRadius: 'var(--radius-sm)',
                  }}>
                    <div style={{ fontSize: '0.7rem', fontWeight: 600, color: 'var(--color-critical)', textTransform: 'uppercase', letterSpacing: '0.04em', marginBottom: 4 }}>
                      Root Cause
                    </div>
                    {selectedPattern.root_cause_explanation}
                  </div>
                )}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, fontSize: '0.8rem' }}>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Occurrences:</span>{' '}
                    <strong>{selectedPattern.occurrence_count}</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Avg MTTR:</span>{' '}
                    <strong>{Math.round(Number(selectedPattern.avg_mttr_minutes))}m</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>P95 MTTR:</span>{' '}
                    <strong>{Math.round(Number(selectedPattern.p95_mttr_minutes))}m</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Max Blast:</span>{' '}
                    <strong>{selectedPattern.max_blast_radius}</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Revenue:</span>{' '}
                    <strong>{formatCurrency(selectedPattern.total_revenue_impact)}</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Users:</span>{' '}
                    <strong>{formatNumber(selectedPattern.total_user_impact)}</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>SLA Breaches:</span>{' '}
                    <strong style={{ color: 'var(--color-critical)' }}>{selectedPattern.sla_breach_count}</strong>
                  </div>
                  <div>
                    <span style={{ color: 'var(--color-text-muted)' }}>Recurrence:</span>{' '}
                    <strong>~{Math.round(Number(selectedPattern.avg_days_between_occurrences))}d</strong>
                  </div>
                </div>
              </div>
              <ResponsiveContainer width="100%" height={250}>
                <RadarChart data={radarData} margin={{ top: 10, right: 30, bottom: 10, left: 30 }}>
                  <PolarGrid stroke="var(--color-border)" />
                  <PolarAngleAxis dataKey="metric" tick={{ fontSize: 10, fill: 'var(--color-text-muted)' }} />
                  <PolarRadiusAxis tick={{ fontSize: 8 }} domain={[0, 100]} />
                  <Radar dataKey="value" stroke="var(--color-accent)" fill="var(--color-accent)" fillOpacity={0.25} />
                </RadarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="card" style={{ textAlign: 'center', padding: '48px 24px', color: 'var(--color-text-muted)' }}>
              <AlertTriangle size={32} style={{ marginBottom: 8, opacity: 0.5 }} />
              <div>Select a failure pattern to view details</div>
            </div>
          )}
        </div>
      </div>

      {/* AI Analysis Result */}
      {(aiResult || aiLoading) && (
        <div className="card" style={{ marginTop: 'var(--spacing-lg)' }}>
          <div className="card-header">
            <span className="card-title" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Sparkles size={16} style={{ color: 'var(--color-accent)' }} />
              AI Root Cause Analysis
              {aiResult?.model && (
                <span style={{
                  fontSize: '0.65rem', color: 'var(--color-text-muted)', fontWeight: 400
                }}>
                  via {aiResult.model}
                </span>
              )}
            </span>
          </div>
          {aiLoading ? (
            <LoadingState message="Generating AI analysis..." />
          ) : aiResult?.analysis ? (
            <div className="markdown-content">
              <ReactMarkdown>{aiResult.analysis}</ReactMarkdown>
            </div>
          ) : null}
          {aiResult?.note && (
            <div style={{
              marginTop: 12, padding: '8px 12px',
              background: 'rgba(210, 153, 34, 0.1)',
              borderRadius: 'var(--radius-sm)',
              fontSize: '0.75rem',
              color: 'var(--color-medium)',
            }}>
              {aiResult.note}
            </div>
          )}
          {aiResult?.email_sent !== undefined && (
            <div style={{
              marginTop: 12, padding: '8px 12px',
              background: aiResult.email_sent ? 'rgba(46, 160, 67, 0.08)' : 'rgba(210, 153, 34, 0.08)',
              borderRadius: 'var(--radius-sm)',
              fontSize: '0.75rem',
              display: 'flex', alignItems: 'center', gap: 6,
              color: aiResult.email_sent ? 'var(--color-healthy)' : 'var(--color-text-muted)',
            }}>
              {aiResult.email_sent
                ? <><Mail size={13} /> Analysis emailed to {aiResult.recipients?.join(', ')}</>
                : <><MailX size={13} /> Email not sent: {aiResult.reason}</>
              }
            </div>
          )}
        </div>
      )}
    </div>
  );
}
