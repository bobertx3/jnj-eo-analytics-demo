import React from 'react';

/**
 * Shared custom tooltip for Recharts charts.
 */
export default function ChartTooltip({ active, payload, label, formatter }) {
  if (!active || !payload || !payload.length) return null;

  return (
    <div style={{
      background: 'var(--color-bg-elevated)',
      border: '1px solid var(--color-border)',
      borderRadius: 'var(--radius-md)',
      padding: '10px 14px',
      boxShadow: 'var(--shadow-md)',
      fontSize: '0.8rem',
    }}>
      <div style={{ color: 'var(--color-text-primary)', fontWeight: 600, marginBottom: 6 }}>
        {label}
      </div>
      {payload.map((item, idx) => (
        <div key={idx} style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          marginBottom: 3,
          color: 'var(--color-text-secondary)',
        }}>
          <span style={{
            width: 8, height: 8, borderRadius: '50%',
            background: item.color, flexShrink: 0,
          }} />
          <span>{item.name}:</span>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)' }}>
            {formatter ? formatter(item.value, item.name) : item.value}
          </span>
        </div>
      ))}
    </div>
  );
}
