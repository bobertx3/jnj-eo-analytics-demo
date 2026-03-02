import React from 'react';

export function LoadingState({ message = 'Loading data...' }) {
  return (
    <div className="loading-container">
      <div className="spinner" />
      <span>{message}</span>
    </div>
  );
}

export function ErrorState({ message = 'Failed to load data', onRetry }) {
  return (
    <div className="error-message" style={{ textAlign: 'center', padding: '24px' }}>
      <div style={{ marginBottom: 8 }}>{message}</div>
      {onRetry && (
        <button className="btn btn-secondary btn-sm" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

export function EmptyState({ message = 'No data available' }) {
  return (
    <div className="loading-container" style={{ color: 'var(--color-text-muted)' }}>
      <span>{message}</span>
    </div>
  );
}
