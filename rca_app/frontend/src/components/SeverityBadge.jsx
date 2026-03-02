import React from 'react';

export function SeverityBadge({ severity }) {
  if (!severity) return null;
  const s = severity.toLowerCase();
  let className = 'badge ';
  if (s === 'p1' || s === 'critical') className += 'badge-p1';
  else if (s === 'p2' || s === 'high') className += 'badge-p2';
  else if (s === 'p3' || s === 'medium') className += 'badge-p3';
  else className += 'badge-low';
  return <span className={className}>{severity}</span>;
}

export function DomainBadge({ domain }) {
  if (!domain) return null;
  const d = domain.toLowerCase();
  let className = 'badge ';
  if (d === 'infrastructure') className += 'badge-infrastructure';
  else if (d === 'application') className += 'badge-application';
  else if (d === 'network') className += 'badge-network';
  else className += 'badge-info';
  return <span className={className}>{domain}</span>;
}

export function TrendBadge({ trend }) {
  if (!trend) return null;
  const t = trend.toLowerCase();
  let className = 'badge badge-' + t;
  return <span className={className}>{trend}</span>;
}
